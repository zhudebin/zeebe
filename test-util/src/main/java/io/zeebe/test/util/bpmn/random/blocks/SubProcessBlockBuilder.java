/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.zeebe.test.util.bpmn.random.blocks;

import io.zeebe.model.bpmn.builder.AbstractFlowNodeBuilder;
import io.zeebe.model.bpmn.builder.ExclusiveGatewayBuilder;
import io.zeebe.model.bpmn.builder.SubProcessBuilder;
import io.zeebe.test.util.bpmn.random.AbstractExecutionStep;
import io.zeebe.test.util.bpmn.random.BlockBuilder;
import io.zeebe.test.util.bpmn.random.BlockBuilderFactory;
import io.zeebe.test.util.bpmn.random.ConstructionContext;
import io.zeebe.test.util.bpmn.random.ExecutionPathSegment;
import io.zeebe.test.util.bpmn.random.IDGenerator;
import io.zeebe.test.util.bpmn.random.RandomProcessGenerator;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Generates an embedded sub process. The embedded sub process contains either a sequence of random
 * blocks or a start event directly connected to the end event
 */
public class SubProcessBlockBuilder implements BlockBuilder {

  private BlockBuilder embeddedSubProcessBuilder;
  private final String subProcessId;
  private final String subProcessStartEventId;
  private final String subProcessEndEventId;
  private final String subProcessBoundaryTimerEventId;

  private final boolean hasBoundaryEvents;
  private final boolean hasBoundaryTimerEvent;

  public SubProcessBlockBuilder(final ConstructionContext context) {
    final Random random = context.getRandom();
    final IDGenerator idGenerator = context.getIdGenerator();
    final BlockSequenceBuilder.BlockSequenceBuilderFactory factory =
        context.getBlockSequenceBuilderFactory();
    final int maxDepth = context.getMaxDepth();
    final int currentDepth = context.getCurrentDepth();

    subProcessId = idGenerator.nextId();
    subProcessStartEventId = idGenerator.nextId();
    subProcessEndEventId = idGenerator.nextId();

    subProcessBoundaryTimerEventId = "boundary_timer_" + subProcessId;

    final boolean goDeeper = random.nextInt(maxDepth) > currentDepth;

    if (goDeeper) {
      embeddedSubProcessBuilder =
          factory.createBlockSequenceBuilder(context.withIncrementedDepth());

      hasBoundaryTimerEvent =
          random.nextDouble() < RandomProcessGenerator.PROBABILITY_BOUNDARY_TIMER_EVENT;
    } else {
      hasBoundaryTimerEvent = false;
    }

    hasBoundaryEvents = hasBoundaryTimerEvent; // extend here
  }

  @Override
  public AbstractFlowNodeBuilder<?, ?> buildFlowNodes(
      final AbstractFlowNodeBuilder<?, ?> nodeBuilder) {
    final SubProcessBuilder subProcessBuilderStart = nodeBuilder.subProcess(subProcessId);

    AbstractFlowNodeBuilder<?, ?> workInProgress =
        subProcessBuilderStart.embeddedSubProcess().startEvent(subProcessStartEventId);

    if (embeddedSubProcessBuilder != null) {
      workInProgress = embeddedSubProcessBuilder.buildFlowNodes(workInProgress);
    }

    final var subProcessBuilderDone =
        workInProgress.endEvent(subProcessEndEventId).subProcessDone();

    AbstractFlowNodeBuilder result = subProcessBuilderDone;
    if (hasBoundaryEvents) {
      final String joinGatewayId = "join_" + subProcessId;
      final ExclusiveGatewayBuilder exclusiveGatewayBuilder =
          subProcessBuilderDone.exclusiveGateway(joinGatewayId);

      if (hasBoundaryTimerEvent) {

        result =
            ((SubProcessBuilder) exclusiveGatewayBuilder.moveToNode(subProcessId))
                .boundaryEvent(
                    subProcessBoundaryTimerEventId,
                    b -> b.timerWithDurationExpression(subProcessBoundaryTimerEventId))
                .connectTo(joinGatewayId);
      }
    }

    return result;
  }

  @Override
  public ExecutionPathSegment findRandomExecutionPath(final Random random) {
    final ExecutionPathSegment result = new ExecutionPathSegment();

    final var enterSubProcessStep =
        new StepEnterSubProcess(subProcessId, subProcessBoundaryTimerEventId);
    result.append(enterSubProcessStep);

    if (embeddedSubProcessBuilder == null) {
      return result;
    }

    final var internalExecutionPath = embeddedSubProcessBuilder.findRandomExecutionPath(random);

    if (internalExecutionPath.getScheduledSteps().isEmpty()) {
      return result;
    }

    if (!hasBoundaryEvents || random.nextBoolean()) {
      result.append(internalExecutionPath);
    } else {
      final int cutOffPoint =
          Math.min(1, random.nextInt(internalExecutionPath.getScheduledSteps().size()));

      for (int i = 0; i < cutOffPoint; i++) {
        result.append(internalExecutionPath.getSteps().get(i));
      }

      if (hasBoundaryTimerEvent) {
        result.append(
            new StepTimeoutSubProcess(subProcessId, subProcessBoundaryTimerEventId),
            enterSubProcessStep);
      } // extend here for other boundary events
    }

    return result;
  }

  public static final class StepEnterSubProcess extends AbstractExecutionStep {

    private final String subProcessId;

    public StepEnterSubProcess(
        final String subProcessId, final String subProcessBoundaryTimerEventId) {
      this.subProcessId = subProcessId;
      /* temporary value to have a timer that will not fire in normal execution; if the execution
       * path includes a StepTimeoutSubProcess, then the value will be overwritten with the correct
       * time for that execution path
       */
      variables.put(subProcessBoundaryTimerEventId, VIRTUALLY_INFINITE.toString());
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
      if (!super.equals(o)) {
        return false;
      }

      final StepEnterSubProcess that = (StepEnterSubProcess) o;

      return subProcessId.equals(that.subProcessId);
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + subProcessId.hashCode();
      return result;
    }
  }

  public static final class StepTimeoutSubProcess extends AbstractExecutionStep {

    private final String subProcessId;
    private final String subProcessBoundaryTimerEventId;

    public StepTimeoutSubProcess(
        final String subProcessId, final String subProcessBoundaryTimerEventId) {
      this.subProcessId = subProcessId;
      this.subProcessBoundaryTimerEventId = subProcessBoundaryTimerEventId;
    }

    public String getSubProcessBoundaryTimerEventId() {
      return subProcessBoundaryTimerEventId;
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
    public Map<String, Object> updateVariables(
        final Map<String, Object> variables, final Duration activationDuration) {
      final var result = new HashMap<>(variables);
      result.put(subProcessBoundaryTimerEventId, activationDuration.toString());
      return result;
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

      final StepTimeoutSubProcess that = (StepTimeoutSubProcess) o;

      return subProcessId.equals(that.subProcessId);
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + subProcessId.hashCode();
      return result;
    }
  }

  public static class Factory implements BlockBuilderFactory {

    @Override
    public BlockBuilder createBlockBuilder(final ConstructionContext context) {
      return new SubProcessBlockBuilder(context);
    }

    @Override
    public boolean isAddingDepth() {
      return true;
    }
  }
}
