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
import io.camunda.zeebe.engine.processing.streamprocessor.writers.StateWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedCommandWriter;
import io.camunda.zeebe.protocol.impl.record.UnifiedRecordValue;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.intent.Intent;
import java.util.function.Consumer;

/**
 * High-level record processor abstraction that implements the common behavior of most
 * command-handling processors.
 */
public interface CommandProcessor<T extends UnifiedRecordValue> {

  default boolean onCommand(final TypedRecord<T> command, final CommandControl<T> commandControl) {
    return true;
  }

  default boolean onCommand(
      final TypedRecord<T> command,
      final CommandControl<T> commandControl,
      final Consumer<SideEffectProducer> sideEffect) {
    return onCommand(command, commandControl);
  }

  // TODO (#8003): clean up after refactoring; this is just a simple hook to be able to append
  // additional commands/events
  default void afterAccept(
      final TypedCommandWriter commandWriter,
      final StateWriter stateWriter,
      final long key,
      final Intent intent,
      final T value) {}

  interface CommandControl<T> {

    /**
     * @return the key of the entity
     */
    long accept(Intent newState, T updatedValue);

    void reject(RejectionType type, String reason);
  }
}
