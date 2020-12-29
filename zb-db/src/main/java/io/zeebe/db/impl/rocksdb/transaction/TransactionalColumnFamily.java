/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.db.impl.rocksdb.transaction;

import io.zeebe.db.ColumnFamily;
import io.zeebe.db.DbContext;
import io.zeebe.db.DbKey;
import io.zeebe.db.DbValue;
import io.zeebe.db.KeyValuePairVisitor;
import io.zeebe.db.impl.DbCompositeKey;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.collections.MutableBoolean;

class TransactionalColumnFamily<
        ColumnFamilyNames extends Enum<ColumnFamilyNames>,
        KeyType extends DbKey,
        ValueType extends DbValue>
    implements ColumnFamily<KeyType, ValueType> {

  private final ZeebeTransactionDb<ColumnFamilyNames> transactionDb;
  private final DbKey keyPrefix;

  private final DbContext context;

  private final ValueType valueInstance;
  private final DbCompositeKey<DbKey, KeyType> prefixedKeyInstance;

  TransactionalColumnFamily(
      final ZeebeTransactionDb<ColumnFamilyNames> transactionDb,
      final DbKey keyPrefix,
      final DbContext context,
      final KeyType keyInstance,
      final ValueType valueInstance) {
    this.transactionDb = transactionDb;
    this.keyPrefix = keyPrefix;
    this.context = context;
    this.valueInstance = valueInstance;

    prefixedKeyInstance = new DbCompositeKey<>(keyPrefix, keyInstance);
  }

  @Override
  public void put(final KeyType key, final ValueType value) {
    put(context, key, value);
  }

  @Override
  public void put(final DbContext context, final KeyType key, final ValueType value) {
    transactionDb.put(context, new DbCompositeKey<>(keyPrefix, key), value);
  }

  @Override
  public ValueType get(final KeyType key) {
    return get(context, key, valueInstance);
  }

  @Override
  public ValueType get(final DbContext context, final KeyType key, final ValueType value) {
    final DbCompositeKey<DbKey, KeyType> prefixedKey = new DbCompositeKey<>(keyPrefix, key);
    final DirectBuffer valueBuffer = transactionDb.get(context, prefixedKey);
    if (valueBuffer != null) {
      value.wrap(valueBuffer, 0, valueBuffer.capacity());
      return value;
    }
    return null;
  }

  @Override
  public void forEach(final Consumer<ValueType> consumer) {
    transactionDb.whileEqualPrefix(
        context,
        keyPrefix,
        prefixedKeyInstance,
        valueInstance,
        (ignored, value) -> {
          consumer.accept(value);
        });
  }

  @Override
  public void forEach(final BiConsumer<KeyType, ValueType> consumer) {
    transactionDb.whileEqualPrefix(
        context,
        keyPrefix,
        prefixedKeyInstance,
        valueInstance,
        (key, value) -> {
          consumer.accept(key.getSecond(), value);
        });
  }

  @Override
  public void whileTrue(final KeyValuePairVisitor<KeyType, ValueType> visitor) {
    transactionDb.whileEqualPrefix(
        context,
        keyPrefix,
        prefixedKeyInstance,
        valueInstance,
        KeyValuePairVisitor.prefixed(visitor));
  }

  @Override
  public void whileTrue(
      final DbContext context,
      final KeyValuePairVisitor<KeyType, ValueType> visitor,
      final KeyType key,
      final ValueType value) {
    final DbCompositeKey<DbKey, KeyType> prefixedKey = new DbCompositeKey<>(keyPrefix, key);
    transactionDb.whileEqualPrefix(
        context, keyPrefix, prefixedKey, value, KeyValuePairVisitor.prefixed(visitor));
  }

  @Override
  public void whileEqualPrefix(
      final DbKey keyPrefix, final BiConsumer<KeyType, ValueType> visitor) {
    final DbCompositeKey<DbKey, DbKey> prefixedKey =
        new DbCompositeKey<>(this.keyPrefix, keyPrefix);

    transactionDb.whileEqualPrefix(
        context,
        prefixedKey,
        prefixedKeyInstance,
        valueInstance,
        (key, value) -> {
          visitor.accept(key.getSecond(), value);
        });
  }

  @Override
  public void whileEqualPrefix(
      final DbKey keyPrefix, final KeyValuePairVisitor<KeyType, ValueType> visitor) {
    transactionDb.whileEqualPrefix(
        context,
        new DbCompositeKey<>(this.keyPrefix, keyPrefix),
        prefixedKeyInstance,
        valueInstance,
        KeyValuePairVisitor.prefixed(visitor));
  }

  @Override
  public void delete(final KeyType key) {
    delete(context, key);
  }

  @Override
  public void delete(final DbContext context, final KeyType key) {
    final DbCompositeKey<DbKey, KeyType> prefixedKey = new DbCompositeKey<>(keyPrefix, key);
    transactionDb.delete(context, prefixedKey);
  }

  @Override
  public boolean exists(final KeyType key) {
    final DbCompositeKey<DbKey, KeyType> prefixedKey = new DbCompositeKey<>(keyPrefix, key);
    return transactionDb.exists(context, prefixedKey);
  }

  @Override
  public boolean isEmpty() {
    return isEmpty(context);
  }

  @Override
  public boolean isEmpty(final DbContext context) {
    final MutableBoolean isEmpty = new MutableBoolean(true);
    transactionDb.whileEqualPrefix(
        context,
        keyPrefix,
        prefixedKeyInstance,
        valueInstance,
        (k, v) -> {
          isEmpty.set(false);
          return false;
        });

    return isEmpty.get();
  }
}
