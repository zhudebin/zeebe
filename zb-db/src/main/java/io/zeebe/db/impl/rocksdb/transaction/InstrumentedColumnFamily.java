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
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public final class InstrumentedColumnFamily<KeyType extends DbKey, ValueType extends DbValue>
    implements ColumnFamily<KeyType, ValueType> {
  private final ColumnFamily<KeyType, ValueType> delegate;

  public InstrumentedColumnFamily(final ColumnFamily<KeyType, ValueType> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void put(final KeyType key, final ValueType value) {
    Instrumentation.PUT.time(() -> delegate.put(key, value));
  }

  @Override
  public void put(final DbContext dbContext, final KeyType key, final ValueType value) {
    Instrumentation.PUT.time(() -> delegate.put(dbContext, key, value));
  }

  @Override
  public ValueType get(final KeyType key) {
    return Instrumentation.GET.time(() -> delegate.get(key));
  }

  @Override
  public ValueType get(final DbContext dbContext, final KeyType key, final ValueType value) {
    return Instrumentation.GET.time(() -> delegate.get(dbContext, key, value));
  }

  @Override
  public void forEach(final Consumer<ValueType> consumer) {
    Instrumentation.FOREACH.time(() -> delegate.forEach(consumer));
  }

  @Override
  public void forEach(final BiConsumer<KeyType, ValueType> consumer) {
    Instrumentation.FOREACH.time(() -> delegate.forEach(consumer));
  }

  @Override
  public void whileTrue(final KeyValuePairVisitor<KeyType, ValueType> visitor) {
    Instrumentation.WHILETRUE.time(() -> delegate.whileTrue(visitor));
  }

  @Override
  public void whileTrue(
      final DbContext dbContext,
      final KeyValuePairVisitor<KeyType, ValueType> visitor,
      final KeyType key,
      final ValueType value) {
    Instrumentation.WHILETRUE.time(() -> delegate.whileTrue(dbContext, visitor, key, value));
  }

  @Override
  public void whileEqualPrefix(
      final DbKey keyPrefix, final BiConsumer<KeyType, ValueType> visitor) {
    Instrumentation.WHILEEQUALPREFIX.time(() -> delegate.whileEqualPrefix(keyPrefix, visitor));
  }

  @Override
  public void whileEqualPrefix(
      final DbKey keyPrefix, final KeyValuePairVisitor<KeyType, ValueType> visitor) {
    Instrumentation.WHILEEQUALPREFIX.time(() -> delegate.whileEqualPrefix(keyPrefix, visitor));
  }

  @Override
  public void delete(final KeyType key) {
    Instrumentation.DELETE.time(() -> delegate.delete(key));
  }

  @Override
  public void delete(final DbContext dbContext, final KeyType key) {
    Instrumentation.DELETE.time(() -> delegate.delete(dbContext, key));
  }

  @Override
  public boolean exists(final KeyType key) {
    return Instrumentation.EXISTS.time(() -> delegate.exists(key));
  }

  @Override
  public boolean isEmpty() {
    return Instrumentation.ISEMPTY.time((Callable<Boolean>) delegate::isEmpty);
  }

  @Override
  public boolean isEmpty(final DbContext dbContext) {
    return Instrumentation.ISEMPTY.time(() -> delegate.isEmpty(dbContext));
  }
}
