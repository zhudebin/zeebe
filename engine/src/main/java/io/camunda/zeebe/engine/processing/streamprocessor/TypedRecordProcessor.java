/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.streamprocessor;

import io.camunda.zeebe.engine.api.TypedRecord;
import io.camunda.zeebe.engine.processing.streamprocessor.sideeffect.SideEffectProducer;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.LegacyTypedResponseWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.LegacyTypedStreamWriter;
import io.camunda.zeebe.protocol.impl.record.UnifiedRecordValue;
import java.util.function.Consumer;

// todo (#8002): remove TypedStreamWriter from this interface's method signatures
// After the migration, none of these should be in use anymore and replaced by the CommandWriter and
// StateWriter passed along to the constructors of the concrete processors. The only method that
// should remain in use is {@code processRecord(final TypedRecord<T> record)}
public interface TypedRecordProcessor<T extends UnifiedRecordValue> {

  default void processRecord(final TypedRecord<T> record) {}

  /**
   * @see #processRecord(TypedRecord, LegacyTypedResponseWriter, LegacyTypedStreamWriter, Consumer)
   */
  default void processRecord(
      final TypedRecord<T> record,
      final LegacyTypedResponseWriter responseWriter,
      final LegacyTypedStreamWriter streamWriter) {
    processRecord(record);
  }

  /**
   * @see #processRecord(TypedRecord, LegacyTypedResponseWriter, LegacyTypedStreamWriter, Consumer)
   */
  default void processRecord(
      final TypedRecord<T> record,
      final LegacyTypedResponseWriter responseWriter,
      final LegacyTypedStreamWriter streamWriter,
      final Consumer<SideEffectProducer> sideEffect) {
    processRecord(record, responseWriter, streamWriter);
  }

  /**
   * @param position the position of the current record to process
   * @param record the record to process
   * @param responseWriter the default side effect that can be used for sending responses. {@link
   *     LegacyTypedResponseWriter#flush()} must not be called in this method.
   * @param streamWriter
   * @param sideEffect consumer to replace the default side effect (response writer). Can be used to
   *     implement other types of side effects or composite side effects. If a composite side effect
   *     involving the response writer is used, {@link LegacyTypedResponseWriter#flush()} must be
   *     called in the {@link SideEffectProducer} implementation.
   */
  default void processRecord(
      final long position,
      final TypedRecord<T> record,
      final LegacyTypedResponseWriter responseWriter,
      final LegacyTypedStreamWriter streamWriter,
      final Consumer<SideEffectProducer> sideEffect) {
    processRecord(record, responseWriter, streamWriter, sideEffect);
  }
}
