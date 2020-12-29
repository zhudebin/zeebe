/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.db.impl;

import static io.zeebe.db.impl.ZeebeDbConstants.ZB_DB_BYTE_ORDER;

import io.zeebe.db.DbKey;
import io.zeebe.db.DbValue;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public final class DbShort implements DbKey, DbValue {
  private short value;

  public DbShort() {}

  public DbShort(final short value) {
    this.value = value;
  }

  public void wrapValue(final short value) {
    this.value = value;
  }

  @Override
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    value = buffer.getShort(offset, ZB_DB_BYTE_ORDER);
  }

  @Override
  public int getLength() {
    return Short.BYTES;
  }

  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    buffer.putShort(offset, value, ZB_DB_BYTE_ORDER);
  }
}
