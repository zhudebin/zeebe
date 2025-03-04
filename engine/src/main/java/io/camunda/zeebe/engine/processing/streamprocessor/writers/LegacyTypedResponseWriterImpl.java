/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.streamprocessor.writers;

import io.camunda.zeebe.engine.api.TypedRecord;
import io.camunda.zeebe.engine.processing.streamprocessor.sideeffect.SideEffectProducer;
import io.camunda.zeebe.msgpack.UnpackedObject;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.Intent;
import java.nio.charset.StandardCharsets;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class LegacyTypedResponseWriterImpl
    implements LegacyTypedResponseWriter, SideEffectProducer {

  private final CommandResponseWriter writer;
  private final int partitionId;
  private final UnsafeBuffer stringWrapper = new UnsafeBuffer(0, 0);
  private long requestId;
  private int requestStreamId;
  private boolean isResponseStaged;

  public LegacyTypedResponseWriterImpl(final CommandResponseWriter writer, final int partitionId) {
    this.writer = writer;
    this.partitionId = partitionId;
  }

  @Override
  public void writeRejectionOnCommand(
      final TypedRecord<?> command, final RejectionType type, final String reason) {
    final byte[] bytes = reason.getBytes(StandardCharsets.UTF_8);
    stringWrapper.wrap(bytes);

    stage(
        RecordType.COMMAND_REJECTION,
        command.getIntent(),
        command.getKey(),
        type,
        stringWrapper,
        command.getValueType(),
        command.getRequestId(),
        command.getRequestStreamId(),
        command.getValue());
  }

  @Override
  public void writeEvent(final TypedRecord<?> event) {
    stringWrapper.wrap(0, 0);

    stage(
        RecordType.EVENT,
        event.getIntent(),
        event.getKey(),
        RejectionType.NULL_VAL,
        stringWrapper,
        event.getValueType(),
        event.getRequestId(),
        event.getRequestStreamId(),
        event.getValue());
  }

  @Override
  public void writeEventOnCommand(
      final long eventKey,
      final Intent eventState,
      final UnpackedObject eventValue,
      final TypedRecord<?> command) {
    stringWrapper.wrap(0, 0);

    stage(
        RecordType.EVENT,
        eventState,
        eventKey,
        RejectionType.NULL_VAL,
        stringWrapper,
        command.getValueType(),
        command.getRequestId(),
        command.getRequestStreamId(),
        eventValue);
  }

  @Override
  public void writeResponse(
      final long eventKey,
      final Intent eventState,
      final UnpackedObject eventValue,
      final ValueType valueType,
      final long requestId,
      final int requestStreamId) {
    stringWrapper.wrap(0, 0);

    stage(
        RecordType.EVENT,
        eventState,
        eventKey,
        RejectionType.NULL_VAL,
        stringWrapper,
        valueType,
        requestId,
        requestStreamId,
        eventValue);
  }

  @Override
  public void writeResponse(
      final RecordType recordType,
      final long key,
      final Intent intent,
      final UnpackedObject value,
      final ValueType valueType,
      final RejectionType rejectionType,
      final String rejectionReason,
      final long requestId,
      final int requestStreamId) {

    final byte[] bytes = rejectionReason.getBytes(StandardCharsets.UTF_8);
    stringWrapper.wrap(bytes);

    stage(
        recordType,
        intent,
        key,
        rejectionType,
        stringWrapper,
        valueType,
        requestId,
        requestStreamId,
        value);
  }

  @Override
  public boolean flush() {
    if (isResponseStaged) {
      writer.tryWriteResponse(requestStreamId, requestId);
    }
    return true;
  }

  @Override
  public void reset() {
    isResponseStaged = false;
  }

  private void stage(
      final RecordType type,
      final Intent intent,
      final long key,
      final RejectionType rejectionType,
      final DirectBuffer rejectionReason,
      final ValueType valueType,
      final long requestId,
      final int requestStreamId,
      final UnpackedObject value) {
    writer
        .partitionId(partitionId)
        .key(key)
        .intent(intent)
        .recordType(type)
        .valueType(valueType)
        .rejectionType(rejectionType)
        .rejectionReason(rejectionReason)
        .valueWriter(value);

    this.requestId = requestId;
    this.requestStreamId = requestStreamId;
    isResponseStaged = true;
  }
}
