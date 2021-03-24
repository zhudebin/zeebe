/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.zeebe.test.util.bpmn.random.steps;

import io.zeebe.test.util.bpmn.random.AbstractExecutionStep;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class StepTimeoutBPMNElement extends AbstractExecutionStep {

  private final String timerEventId;

  public StepTimeoutBPMNElement(final String timerEventId) {
    this.timerEventId = timerEventId;
  }

  public String getTimerEventId() {
    return timerEventId;
  }

  @Override
  protected Map<String, Object> updateVariables(
      final Map<String, Object> variables, final Duration activationDuration) {
    final var result = new HashMap<>(variables);
    result.put(timerEventId, activationDuration.toString());
    return result;
  }

  @Override
  public boolean isAutomatic() {
    return false;
  }

  @Override
  public Duration getDeltaTime() {
    return DEFAULT_DELTA;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final StepTimeoutBPMNElement that = (StepTimeoutBPMNElement) o;
    return timerEventId.equals(that.timerEventId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), timerEventId);
  }
}
