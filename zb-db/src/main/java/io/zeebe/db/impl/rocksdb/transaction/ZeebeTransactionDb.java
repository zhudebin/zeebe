/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.db.impl.rocksdb.transaction;

import static io.zeebe.db.impl.rocksdb.transaction.RocksDbInternal.getNativeHandle;
import static io.zeebe.util.buffer.BufferUtil.startsWith;

import io.zeebe.db.ColumnFamily;
import io.zeebe.db.DbContext;
import io.zeebe.db.DbKey;
import io.zeebe.db.DbValue;
import io.zeebe.db.KeyValuePairVisitor;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.ZeebeDbException;
import io.zeebe.db.impl.DbNil;
import io.zeebe.db.impl.rocksdb.Loggers;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.agrona.DirectBuffer;
import org.agrona.collections.MutableBoolean;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;

public class ZeebeTransactionDb<ColumnFamilyNames extends Enum<ColumnFamilyNames>>
    implements ZeebeDb<ColumnFamilyNames> {

  private static final Logger LOG = Loggers.DB_LOGGER;
  private static final String ERROR_MESSAGE_CLOSE_RESOURCE =
      "Expected to close RocksDB resource successfully, but exception was thrown. Will continue to close remaining resources.";
  private final OptimisticTransactionDB optimisticTransactionDB;
  private final List<AutoCloseable> closables;
  private final ReadOptions prefixReadOptions;
  private final ReadOptions defaultReadOptions;
  private final WriteOptions defaultWriteOptions;
  private final long nativeHandle;
  private final ColumnFamilyHandle handle;

  private final Map<ColumnFamilyNames, DbKey> columnPrefixMap;
  private final DbKey prefixKeyInstance;

  protected ZeebeTransactionDb(
      final OptimisticTransactionDB optimisticTransactionDB,
      final long nativeHandle,
      final ColumnFamilyHandle handle,
      final Map<ColumnFamilyNames, DbKey> columnPrefixMap,
      final DbKey prefixKeyInstance,
      final List<AutoCloseable> closables) {
    this.optimisticTransactionDB = optimisticTransactionDB;
    this.nativeHandle = nativeHandle;
    this.handle = handle;
    this.columnPrefixMap = columnPrefixMap;
    this.prefixKeyInstance = prefixKeyInstance;
    this.closables = closables;

    prefixReadOptions =
        new ReadOptions()
            .setPrefixSameAsStart(true)
            .setTotalOrderSeek(false)
            // setting a positive value to readahead is only useful when using network storage with
            // high latency, at the cost of making iterators expensiver (memory and computation
            // wise)
            .setReadaheadSize(0);
    closables.add(prefixReadOptions);
    defaultReadOptions = new ReadOptions();
    closables.add(defaultReadOptions);
    defaultWriteOptions = new WriteOptions().setDisableWAL(true);
    closables.add(defaultWriteOptions);
  }

  public static <ColumnFamilyNames extends Enum<ColumnFamilyNames>>
      ZeebeTransactionDb<ColumnFamilyNames> openTransactionalDb(
          final Options options,
          final String path,
          final Map<ColumnFamilyNames, DbKey> columnPrefixMap,
          final DbKey prefixKeyInstance,
          final List<AutoCloseable> closables)
          throws RocksDBException {
    final OptimisticTransactionDB optimisticTransactionDB =
        OptimisticTransactionDB.open(options, path);
    closables.add(optimisticTransactionDB);
    final ColumnFamilyHandle handle = optimisticTransactionDB.getDefaultColumnFamily();
    closables.add(handle);

    final var cfHandle = getNativeHandle(handle);
    return new ZeebeTransactionDb<>(
        optimisticTransactionDB, cfHandle, handle, columnPrefixMap, prefixKeyInstance, closables);
  }

  @Override
  public <KeyType extends DbKey, ValueType extends DbValue>
      ColumnFamily<KeyType, ValueType> createColumnFamily(
          final ColumnFamilyNames columnFamily,
          final DbContext context,
          final KeyType keyInstance,
          final ValueType valueInstance) {
    return new InstrumentedColumnFamily<>(
        new TransactionalColumnFamily<>(
            this, columnPrefixMap.get(columnFamily), context, keyInstance, valueInstance));
  }

  @Override
  public void createSnapshot(final File snapshotDir) {
    try (final Checkpoint checkpoint = Checkpoint.create(optimisticTransactionDB)) {
      try {
        checkpoint.createCheckpoint(snapshotDir.getAbsolutePath());
      } catch (final RocksDBException rocksException) {
        throw new ZeebeDbException(
            String.format("Failed to take snapshot in path %s.", snapshotDir), rocksException);
      }
    }
  }

  @Override
  public Optional<String> getProperty(final String propertyName) {
    String propertyValue = null;
    try {
      propertyValue = optimisticTransactionDB.getProperty(handle, propertyName);
    } catch (final RocksDBException rde) {
      LOG.debug(rde.getMessage(), rde);
    }
    return Optional.ofNullable(propertyValue);
  }

  ////////////////////////////////////////////////////////////////////
  //////////////////////////// GET ///////////////////////////////////
  ////////////////////////////////////////////////////////////////////

  @Override
  public DbContext createContext() {
    final Transaction transaction = optimisticTransactionDB.beginTransaction(defaultWriteOptions);
    final ZeebeTransaction zeebeTransaction = new ZeebeTransaction(transaction);
    closables.add(zeebeTransaction);
    return new DefaultDbContext(zeebeTransaction);
  }

  @Override
  public boolean isEmpty(final ColumnFamilyNames columnFamilyName, final DbContext context) {
    final MutableBoolean isEmpty = new MutableBoolean(true);
    final DbKey keyPrefix = columnPrefixMap.get(columnFamilyName);

    whileEqualPrefix(
        context,
        keyPrefix,
        prefixKeyInstance,
        DbNil.INSTANCE,
        (ignoredKey, ignoredValue) -> {
          isEmpty.set(false);
          return false;
        });

    return isEmpty.get();
  }

  protected void put(final DbContext context, final DbKey key, final DbValue value) {
    ensureInOpenTransaction(
        context,
        transaction -> {
          context.writeKey(key);
          context.writeValue(value);

          transaction.put(
              nativeHandle,
              context.getKeyBufferArray(),
              key.getLength(),
              context.getValueBufferArray(),
              value.getLength());
        });
  }

  private void ensureInOpenTransaction(
      final DbContext context, final TransactionConsumer operation) {
    context.runInTransaction(
        () -> operation.run((ZeebeTransaction) context.getCurrentTransaction()));
  }

  protected DirectBuffer get(final DbContext context, final DbKey key) {
    context.writeKey(key);
    final int keyLength = key.getLength();
    return getValue(context, keyLength);
  }

  ////////////////////////////////////////////////////////////////////
  //////////////////////////// ITERATION /////////////////////////////
  ////////////////////////////////////////////////////////////////////

  private DirectBuffer getValue(final DbContext context, final int keyLength) {
    ensureInOpenTransaction(
        context,
        transaction -> {
          final byte[] value =
              transaction.get(
                  nativeHandle,
                  getNativeHandle(defaultReadOptions),
                  context.getKeyBufferArray(),
                  keyLength);
          context.wrapValueView(value);
        });
    return context.getValueView();
  }

  protected boolean exists(final DbContext context, final DbKey key) {
    context.wrapValueView(new byte[0]);
    ensureInOpenTransaction(
        context,
        transaction -> {
          context.writeKey(key);
          getValue(context, key.getLength());
        });
    return !context.isValueViewEmpty();
  }

  ////////////////////////////////////////////////////////////////////
  //////////////////////////// ITERATION /////////////////////////////
  ////////////////////////////////////////////////////////////////////

  protected void delete(final DbContext context, final DbKey key) {
    context.writeKey(key);

    ensureInOpenTransaction(
        context,
        transaction ->
            transaction.delete(nativeHandle, context.getKeyBufferArray(), key.getLength()));
  }

  ////////////////////////////////////////////////////////////////////
  //////////////////////////// ITERATION /////////////////////////////
  ////////////////////////////////////////////////////////////////////

  RocksIterator newIterator(final DbContext context, final ReadOptions options) {
    return context.newIterator(options, handle);
  }

  protected <KeyType extends DbKey, ValueType extends DbValue> void whileEqualPrefix(
      final DbContext context,
      final DbKey prefix,
      final KeyType keyInstance,
      final ValueType valueInstance,
      final BiConsumer<KeyType, ValueType> visitor) {
    whileEqualPrefix(
        context,
        prefix,
        keyInstance,
        valueInstance,
        (k, v) -> {
          visitor.accept(k, v);
          return true;
        });
  }

  /**
   * NOTE: it doesn't seem possible in Java RocksDB to set a flexible prefix extractor on iterators
   * at the moment, so using prefixes seem to be mostly related to skipping files that do not
   * contain keys with the given prefix (which is useful anyway), but it will still iterate over all
   * keys contained in those files, so we still need to make sure the key actually matches the
   * prefix.
   *
   * <p>While iterating over subsequent keys we have to validate it.
   */
  protected <KeyType extends DbKey, ValueType extends DbValue> void whileEqualPrefix(
      final DbContext context,
      final DbKey prefix,
      final KeyType keyInstance,
      final ValueType valueInstance,
      final KeyValuePairVisitor<KeyType, ValueType> visitor) {
    context.withPrefixKeyBuffer(
        prefixKeyBuffer ->
            ensureInOpenTransaction(
                context,
                transaction -> {
                  try (final RocksIterator iterator = newIterator(context, prefixReadOptions)) {
                    final int prefixLength = prefix.getLength();
                    final ByteBuffer seekBuffer =
                        prefixKeyBuffer.byteBuffer().asReadOnlyBuffer().limit(prefixLength);
                    prefix.write(prefixKeyBuffer, 0);

                    boolean shouldVisitNext = true;

                    for (Instrumentation.SEEK.time(() -> iterator.seek(seekBuffer));
                        iterator.isValid() && shouldVisitNext;
                        iterator.next()) {
                      final byte[] keyBytes = iterator.key();
                      if (!startsWith(
                          prefixKeyBuffer, 0, prefix.getLength(), keyBytes, 0, keyBytes.length)) {
                        break;
                      }

                      shouldVisitNext =
                          visit(context, keyInstance, valueInstance, visitor, iterator);
                    }
                  }
                }));
  }

  private <KeyType extends DbKey, ValueType extends DbValue> boolean visit(
      final DbContext context,
      final KeyType keyInstance,
      final ValueType valueInstance,
      final KeyValuePairVisitor<KeyType, ValueType> iteratorConsumer,
      final RocksIterator iterator) {
    context.wrapKeyView(iterator.key());
    context.wrapValueView(iterator.value());

    final DirectBuffer keyViewBuffer = context.getKeyView();
    keyInstance.wrap(keyViewBuffer, 0, keyViewBuffer.capacity());
    final DirectBuffer valueViewBuffer = context.getValueView();
    valueInstance.wrap(valueViewBuffer, 0, valueViewBuffer.capacity());

    return iteratorConsumer.visit(keyInstance, valueInstance);
  }

  @Override
  public void close() {
    // Correct order of closing
    // 1. transaction
    // 2. options
    // 3. column family handles
    // 4. database
    // 5. db options
    // 6. column family options
    // https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
    Collections.reverse(closables);
    closables.forEach(
        closable -> {
          try {
            closable.close();
          } catch (final Exception e) {
            LOG.error(ERROR_MESSAGE_CLOSE_RESOURCE, e);
          }
        });
  }

  @FunctionalInterface
  interface TransactionConsumer {
    void run(ZeebeTransaction transaction) throws Exception;
  }
}
