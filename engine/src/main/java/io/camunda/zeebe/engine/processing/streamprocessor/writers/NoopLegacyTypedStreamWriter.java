/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.streamprocessor.writers;

import io.camunda.zeebe.engine.api.TypedRecord;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.RecordValue;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.intent.Intent;

public final class NoopLegacyTypedStreamWriter implements LegacyTypedStreamWriter {

  @Override
  public void appendRejection(
      final TypedRecord<? extends RecordValue> command,
      final RejectionType type,
      final String reason) {
    // no op implementation
  }

  @Override
  public void appendRecord(
      final long key,
      final RecordType type,
      final Intent intent,
      final RejectionType rejectionType,
      final String rejectionReason,
      final RecordValue value) {
    // no op implementation
  }

  @Override
  public void configureSourceContext(final long sourceRecordPosition) {
    // no op implementation
  }

  @Override
  public void appendFollowUpEvent(final long key, final Intent intent, final RecordValue value) {
    // no op implementation
  }

  @Override
  public int getMaxEventLength() {
    return Integer.MAX_VALUE;
  }

  @Override
  public void appendNewCommand(final Intent intent, final RecordValue value) {
    // no op implementation
  }

  @Override
  public void appendFollowUpCommand(final long key, final Intent intent, final RecordValue value) {
    // no op implementation
  }

  @Override
  public void reset() {
    // no op implementation
  }

  @Override
  public long flush() {
    return 0;
  }
}
