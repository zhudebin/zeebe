/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.zeebe.test.util.bpmn.random;

import io.zeebe.test.util.bpmn.random.blocks.IntermediateMessageCatchEventBlockBuilder.StepPublishMessage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Segment of an execution path. This will not execute a process start to finish but only covers a
 * part of the process.
 *
 * <p>Execution path segments are mutable
 */
public final class ExecutionPathSegment {

  private final List<AbstractExecutionStep> steps = new ArrayList<>();

  public void append(final AbstractExecutionStep executionStep) {
    steps.add(executionStep);
  }

  public void append(final ExecutionPathSegment pathToAdd) {
    steps.addAll(pathToAdd.getSteps());
  }

  public void replace(final int index, final AbstractExecutionStep executionStep) {
    steps.subList(index, steps.size()).clear();
    steps.add(executionStep);
  }

  public void insert(final int index, final StepPublishMessage stepPublishMessage) {
    steps.add(index, stepPublishMessage);
  }

  public List<AbstractExecutionStep> getSteps() {
    return Collections.unmodifiableList(steps);
  }

  public Map<String, Object> collectVariables() {
    final Map<String, Object> result = new HashMap<>();

    steps.forEach(step -> result.putAll(step.getVariables()));

    return result;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ExecutionPathSegment that = (ExecutionPathSegment) o;

    return steps.equals(that.steps);
  }

  @Override
  public int hashCode() {
    return steps.hashCode();
  }
}
