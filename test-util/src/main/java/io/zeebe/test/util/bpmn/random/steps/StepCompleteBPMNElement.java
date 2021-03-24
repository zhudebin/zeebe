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
import java.util.Map;
import java.util.Objects;

public class StepCompleteBPMNElement extends AbstractExecutionStep {

  private final String elementId;

  public StepCompleteBPMNElement(final String elementId) {
    this.elementId = elementId;
  }

  @Override
  protected Map<String, Object> updateVariables(
      final Map<String, Object> variables, final Duration activationDuration) {
    return variables;
  }

  @Override
  public boolean isAutomatic() {
    return true;
  }

  @Override
  public Duration getDeltaTime() {
    return VIRTUALLY_NO_TIME;
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
    final StepCompleteBPMNElement that = (StepCompleteBPMNElement) o;
    return elementId.equals(that.elementId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), elementId);
  }
}
