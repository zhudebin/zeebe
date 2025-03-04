/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.streamprocessor.writers;

import io.camunda.zeebe.engine.api.ProcessingResultBuilder;
import io.camunda.zeebe.engine.state.EventApplier;
import java.util.function.Supplier;

/** Convenience class to aggregate all the writers */
public final class Writers {

  private final TypedCommandWriter commandWriter;
  private final TypedRejectionWriter rejectionWriter;
  private final StateWriter stateWriter;

  private final TypedResponseWriter responseWriter;
  private final Supplier<ProcessingResultBuilder> resultBuilderSupplier;

  public Writers(
      final Supplier<ProcessingResultBuilder> resultBuilderSupplier,
      final EventApplier eventApplier) {

    this.resultBuilderSupplier = resultBuilderSupplier;
    commandWriter = new ResultBuilderBackedTypedCommandWriter(resultBuilderSupplier);
    rejectionWriter = new ResultBuilderBackedRejectionWriter(resultBuilderSupplier);
    stateWriter =
        new ResultBuilderBackedEventApplyingStateWriter(resultBuilderSupplier, eventApplier);

    responseWriter = new ResultBuilderBackedTypedResponseWriter(resultBuilderSupplier);
  }

  /**
   * @return the writer, which is used by the processors to write (follow-up) commands
   */
  public TypedCommandWriter command() {
    return commandWriter;
  }

  /**
   * @return the writer, which is used by the processors to write command rejections
   */
  public TypedRejectionWriter rejection() {
    return rejectionWriter;
  }

  /**
   * @return the writer of events that also changes state for each event it writes
   */
  public StateWriter state() {
    return stateWriter;
  }

  /**
   * Note: {@code flush()} must not be called on the response writer object. This is done centrally
   *
   * @return the response writer, which is used during processing
   */
  public TypedResponseWriter response() {
    return responseWriter;
  }

  /** Resets written records, response and post commit tasks */
  public void reset() {
    resultBuilderSupplier.get().reset();
  }
}
