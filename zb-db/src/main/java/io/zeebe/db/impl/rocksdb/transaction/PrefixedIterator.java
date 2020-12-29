/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.db.impl.rocksdb.transaction;

import java.nio.ByteBuffer;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksIteratorInterface;

public class PrefixedIterator implements RocksIteratorInterface {
  private final RocksIterator delegate;

  public PrefixedIterator(final RocksIterator delegate) {
    this.delegate = delegate;
  }

  public byte[] key() {
    return delegate.key();
  }

  public int key(final ByteBuffer key) {
    return delegate.key(key);
  }

  public byte[] value() {
    return delegate.value();
  }

  public int value(final ByteBuffer value) {
    return delegate.value(value);
  }

  @Override
  public boolean isValid() {
    return delegate.isValid();
  }

  @Override
  public void seekToFirst() {
    delegate.seekToFirst();
  }

  @Override
  public void seekToLast() {
    delegate.seekToLast();
  }

  @Override
  public void seek(final byte[] target) {
    delegate.seek(target);
  }

  @Override
  public void seekForPrev(final byte[] target) {
    delegate.seekForPrev(target);
  }

  @Override
  public void seek(final ByteBuffer target) {
    delegate.seek(target);
  }

  @Override
  public void seekForPrev(final ByteBuffer target) {
    delegate.seekForPrev(target);
  }

  @Override
  public void next() {
    delegate.next();
  }

  @Override
  public void prev() {
    delegate.prev();
  }

  @Override
  public void status() throws RocksDBException {
    delegate.status();
  }

  @Override
  public void refresh() throws RocksDBException {
    delegate.refresh();
  }
}
