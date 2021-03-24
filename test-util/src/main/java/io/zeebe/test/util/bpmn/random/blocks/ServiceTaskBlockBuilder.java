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
import io.zeebe.model.bpmn.builder.ServiceTaskBuilder;
import io.zeebe.test.util.bpmn.random.AbstractExecutionStep;
import io.zeebe.test.util.bpmn.random.BlockBuilder;
import io.zeebe.test.util.bpmn.random.BlockBuilderFactory;
import io.zeebe.test.util.bpmn.random.ConstructionContext;
import io.zeebe.test.util.bpmn.random.ExecutionPathSegment;
import io.zeebe.test.util.bpmn.random.IDGenerator;
import io.zeebe.test.util.bpmn.random.RandomProcessGenerator;
import io.zeebe.test.util.bpmn.random.steps.StepActivateBPMNElement;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

/** Generates a service task. The service task may have boundary events */
public class ServiceTaskBlockBuilder implements BlockBuilder {

  private final String serviceTaskId;
  private final String jobType;
  private final String errorCode;
  private final String boundaryErrorEventId;
  private final String boundaryTimerEventId;

  private final boolean hasBoundaryEvents;
  private final boolean hasBoundaryErrorEvent;
  private final boolean hasBoundaryTimerEvent;

  public ServiceTaskBlockBuilder(final IDGenerator idGenerator, final Random random) {
    serviceTaskId = idGenerator.nextId();
    jobType = "job_" + serviceTaskId;
    errorCode = "error_" + serviceTaskId;

    boundaryErrorEventId = "boundary_error_" + serviceTaskId;
    boundaryTimerEventId = "boundary_timer_" + serviceTaskId;

    hasBoundaryErrorEvent =
        random.nextDouble() < RandomProcessGenerator.PROBABILITY_BOUNDARY_ERROR_EVENT;
    hasBoundaryTimerEvent =
        random.nextDouble() < RandomProcessGenerator.PROBABILITY_BOUNDARY_TIMER_EVENT;

    hasBoundaryEvents =
        hasBoundaryErrorEvent
            || hasBoundaryTimerEvent; // extend here for additional boundary events
  }

  @Override
  public AbstractFlowNodeBuilder<?, ?> buildFlowNodes(
      final AbstractFlowNodeBuilder<?, ?> nodeBuilder) {

    final ServiceTaskBuilder serviceTaskBuilder = nodeBuilder.serviceTask(serviceTaskId);

    serviceTaskBuilder.zeebeJobRetries("3");

    serviceTaskBuilder.zeebeJobType(jobType);

    AbstractFlowNodeBuilder<?, ?> result = serviceTaskBuilder;

    if (hasBoundaryEvents) {
      final String joinGatewayId = "join_" + serviceTaskId;
      final ExclusiveGatewayBuilder exclusiveGatewayBuilder =
          serviceTaskBuilder.exclusiveGateway(joinGatewayId);

      if (hasBoundaryErrorEvent) {
        result =
            ((ServiceTaskBuilder) exclusiveGatewayBuilder.moveToNode(serviceTaskId))
                .boundaryEvent(boundaryErrorEventId, b -> b.error(errorCode))
                .connectTo(joinGatewayId);
      }

      if (hasBoundaryTimerEvent) {
        result =
            ((ServiceTaskBuilder) exclusiveGatewayBuilder.moveToNode(serviceTaskId))
                .boundaryEvent(
                    boundaryTimerEventId, b -> b.timerWithDurationExpression(boundaryTimerEventId))
                .connectTo(joinGatewayId);
      }
    }

    return result;
  }

  /**
   * This generates a sequence of one or more steps. The final step is always a successful
   * activation and complete cycle. The steps before are randomly determined failed attempts.
   */
  @Override
  public ExecutionPathSegment findRandomExecutionPath(final Random random) {
    final ExecutionPathSegment result = new ExecutionPathSegment();

    final var activateStep = new StepActivateBPMNElement(serviceTaskId);
    result.append(activateStep);

    if (hasBoundaryTimerEvent) {
      // set an infinite timer as default; this can be overwritten by the execution path chosen
      result.setVariableDefault(
          boundaryTimerEventId, AbstractExecutionStep.VIRTUALLY_INFINITE.toString());
    }

    result.append(buildStepsForFailedExecutions(random));

    result.append(buildStepForSuccessfulExecution(random), activateStep);

    return result;
  }

  private ExecutionPathSegment buildStepsForFailedExecutions(final Random random) {
    final ExecutionPathSegment result = new ExecutionPathSegment();

    if (random.nextBoolean()) {
      result.append(new StepActivateAndTimeoutJob(jobType));
    }

    if (random.nextBoolean()) {
      final boolean updateRetries = random.nextBoolean();
      result.append(new StepActivateAndFailJob(jobType, updateRetries));
    }

    return result;
  }

  /**
   * This method build the step that results in a successful execution of the service task.
   * Successful execution here does not necessarily mean that the job is completed orderly.
   * Successful execution is any execution which moves the token past the service task, so that the
   * process can continue.
   */
  private AbstractExecutionStep buildStepForSuccessfulExecution(final Random random) {
    final AbstractExecutionStep result;

    if (hasBoundaryErrorEvent && random.nextBoolean()) {
      result = new StepActivateJobAndThrowError(jobType, errorCode);
    } else if (hasBoundaryTimerEvent && random.nextBoolean()) {
      result = new StepTimeoutServiceTask(jobType, boundaryTimerEventId);
    } else {
      result = new StepActivateAndCompleteJob(jobType);
    }

    return result;
  }

  public static final class StepActivateAndCompleteJob extends AbstractExecutionStep {
    private final String jobType;

    public StepActivateAndCompleteJob(final String jobType) {
      this.jobType = jobType;
    }

    public String getJobType() {
      return jobType;
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

      final StepActivateAndCompleteJob that = (StepActivateAndCompleteJob) o;

      if (jobType != null ? !jobType.equals(that.jobType) : that.jobType != null) {
        return false;
      }
      return variables.equals(that.variables);
    }

    @Override
    public int hashCode() {
      int result = jobType != null ? jobType.hashCode() : 0;
      result = 31 * result + variables.hashCode();
      return result;
    }
  }

  public static final class StepActivateAndFailJob extends AbstractExecutionStep {
    private final String jobType;
    private final boolean updateRetries;

    public StepActivateAndFailJob(final String jobType, final boolean updateRetries) {
      this.jobType = jobType;
      this.updateRetries = updateRetries;
    }

    public String getJobType() {
      return jobType;
    }

    public boolean isUpdateRetries() {
      return updateRetries;
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

      final StepActivateAndFailJob that = (StepActivateAndFailJob) o;

      if (updateRetries != that.updateRetries) {
        return false;
      }
      if (jobType != null ? !jobType.equals(that.jobType) : that.jobType != null) {
        return false;
      }
      return variables.equals(that.variables);
    }

    @Override
    public int hashCode() {
      int result = jobType != null ? jobType.hashCode() : 0;
      result = 31 * result + (updateRetries ? 1 : 0);
      result = 31 * result + variables.hashCode();
      return result;
    }
  }

  public static final class StepActivateAndTimeoutJob extends AbstractExecutionStep {
    private final String jobType;

    public StepActivateAndTimeoutJob(final String jobType) {
      this.jobType = jobType;
    }

    public String getJobType() {
      return jobType;
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

      final StepActivateAndTimeoutJob that = (StepActivateAndTimeoutJob) o;

      if (jobType != null ? !jobType.equals(that.jobType) : that.jobType != null) {
        return false;
      }
      return variables.equals(that.variables);
    }

    @Override
    public int hashCode() {
      int result = jobType != null ? jobType.hashCode() : 0;
      result = 31 * result + variables.hashCode();
      return result;
    }
  }

  public static class StepActivateJobAndThrowError extends AbstractExecutionStep {

    private final String jobType;
    private final String errorCode;

    public StepActivateJobAndThrowError(final String jobType, final String errorCode) {
      super();
      this.jobType = jobType;
      this.errorCode = errorCode;
    }

    @Override
    public boolean isAutomatic() {
      return false;
    }

    public String getJobType() {
      return jobType;
    }

    public String getErrorCode() {
      return errorCode;
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

      final StepActivateJobAndThrowError that = (StepActivateJobAndThrowError) o;

      if (jobType != null ? !jobType.equals(that.jobType) : that.jobType != null) {
        return false;
      }
      if (errorCode != null ? !errorCode.equals(that.errorCode) : that.errorCode != null) {
        return false;
      }
      return variables.equals(that.variables);
    }

    @Override
    public int hashCode() {
      int result = jobType != null ? jobType.hashCode() : 0;
      result = errorCode != null ? errorCode.hashCode() : 0;
      result = 31 * result + variables.hashCode();
      return result;
    }
  }

  public static final class StepTimeoutServiceTask extends AbstractExecutionStep {

    private final String jobType;
    private final String boundaryTimerEventId;

    public StepTimeoutServiceTask(final String jobType, final String boundaryTimerEventId) {
      this.jobType = jobType;
      this.boundaryTimerEventId = boundaryTimerEventId;
    }

    @Override
    protected Map<String, Object> updateVariables(
        final Map<String, Object> variables, final Duration activationDuration) {
      final var result = new HashMap<>(variables);
      result.put(boundaryTimerEventId, activationDuration.toString());
      return result;
    }

    public String getBoundaryTimerEventId() {
      return boundaryTimerEventId;
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
      final StepTimeoutServiceTask that = (StepTimeoutServiceTask) o;
      return jobType.equals(that.jobType) && boundaryTimerEventId.equals(that.boundaryTimerEventId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), jobType, boundaryTimerEventId);
    }
  }

  public static class Factory implements BlockBuilderFactory {

    @Override
    public BlockBuilder createBlockBuilder(final ConstructionContext context) {
      return new ServiceTaskBlockBuilder(context.getIdGenerator(), context.getRandom());
    }

    @Override
    public boolean isAddingDepth() {
      return false;
    }
  }
}
