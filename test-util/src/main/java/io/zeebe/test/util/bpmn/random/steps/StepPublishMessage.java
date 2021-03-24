/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.zeebe.test.util.bpmn.random.steps;

import io.zeebe.test.util.bpmn.random.AbstractExecutionStep;
import io.zeebe.test.util.bpmn.random.blocks.IntermediateMessageCatchEventBlockBuilder;
import java.time.Duration;
import java.util.Map;

public final class StepPublishMessage extends AbstractExecutionStep {

  private final String messageName;

  public StepPublishMessage(final String messageName) {
    this.messageName = messageName;
    variables.put(
        IntermediateMessageCatchEventBlockBuilder.CORRELATION_KEY_FIELD,
        IntermediateMessageCatchEventBlockBuilder.CORRELATION_KEY_VALUE);
  }

  public String getMessageName() {
    return messageName;
  }

  @Override
  public boolean isAutomatic() {
    return false;
  }

  @Override
  public Duration getDeltaTime() {
    return VIRTUALLY_NO_TIME;
  }

  @Override
  public Map<String, Object> updateVariables(
      final Map<String, Object> variables, final Duration activationDuration) {
    return variables;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final StepPublishMessage that = (StepPublishMessage) o;

    if (messageName != null ? !messageName.equals(that.messageName) : that.messageName != null) {
      return false;
    }
    return variables.equals(that.variables);
  }

  @Override
  public int hashCode() {
    int result = messageName != null ? messageName.hashCode() : 0;
    result = 31 * result + variables.hashCode();
    return result;
  }
}
