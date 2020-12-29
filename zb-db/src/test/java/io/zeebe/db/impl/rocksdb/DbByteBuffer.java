/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.db.impl.rocksdb;

import io.zeebe.db.DbKey;
import io.zeebe.db.DbValue;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class DbByteBuffer implements DbKey, DbValue {
  private final DirectBuffer buffer;

  public DbByteBuffer(final DirectBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    this.buffer.wrap(buffer, offset, length);
  }

  @Override
  public int getLength() {
    return buffer.capacity();
  }

  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    buffer.putBytes(offset, this.buffer, 0, this.buffer.capacity());
  }
}
