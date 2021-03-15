/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.zeebe.test.util.bpmn.random.blocks;

import static io.zeebe.test.util.bpmn.random.blocks.IntermediateMessageCatchEventBlockBuilder.CORRELATION_KEY_VALUE;

import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.model.bpmn.builder.AbstractFlowNodeBuilder;
import io.zeebe.test.util.bpmn.random.BlockBuilder;
import io.zeebe.test.util.bpmn.random.ConstructionContext;
import io.zeebe.test.util.bpmn.random.ExecutionPath;
import io.zeebe.test.util.bpmn.random.StartEventBlockBuilder;
import io.zeebe.test.util.bpmn.random.blocks.IntermediateMessageCatchEventBlockBuilder.StepPublishMessage;
import io.zeebe.test.util.bpmn.random.blocks.MessageStartEventBuilder.StepPublishStartMessage;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

public final class ProcessBuilder {

  private static final List<Function<ConstructionContext, StartEventBlockBuilder>>
      START_EVENT_BUILDER_FACTORIES =
          List.of(NoneStartEventBuilder::new, MessageStartEventBuilder::new);

  private final BlockBuilder blockBuilder;
  private final StartEventBlockBuilder startEventBuilder;

  private final String processId;
  private final String endEventId;
  private final boolean hasEventSubProcess;
  private String eventSubProcessId = null;
  private boolean isEventSubProcessInterrupting;
  private String eventSubProcessMessageName;

  public ProcessBuilder(final ConstructionContext context) {
    blockBuilder = context.getBlockSequenceBuilderFactory().createBlockSequenceBuilder(context);

    final var idGenerator = context.getIdGenerator();
    processId = "process_" + idGenerator.nextId();
    hasEventSubProcess = true; // context.getRandom().nextBoolean();
    if (hasEventSubProcess) {
      eventSubProcessId = "eventSubProcess_" + idGenerator.nextId();
      isEventSubProcessInterrupting = true; // context.getRandom().nextBoolean();
      eventSubProcessMessageName = "message_" + eventSubProcessId;
    }
    final var random = context.getRandom();
    final var startEventBuilderFactory =
        START_EVENT_BUILDER_FACTORIES.get(random.nextInt(START_EVENT_BUILDER_FACTORIES.size()));
    startEventBuilder = startEventBuilderFactory.apply(context);

    endEventId = idGenerator.nextId();
  }

  public BpmnModelInstance buildProcess() {

    final io.zeebe.model.bpmn.builder.ProcessBuilder processBuilder =
        Bpmn.createExecutableProcess(processId);

    if (hasEventSubProcess) {
      processBuilder
          .eventSubProcess(eventSubProcessId)
          .startEvent("start_event_" + eventSubProcessId)
          .interrupting(isEventSubProcessInterrupting)
          .message(
              b ->
                  // https://github.com/camunda-cloud/zeebe/issues/4099
                  // we might be not able to correlate a message when we have a message start event
                  // in the process
                  // as a workaround we use a constant string here
                  b.name(eventSubProcessMessageName)
                      .zeebeCorrelationKeyExpression('\"' + CORRELATION_KEY_VALUE + '\"'))
          .endEvent("end_event_" + eventSubProcessId);
    }

    AbstractFlowNodeBuilder<?, ?> processWorkInProgress =
        startEventBuilder.buildStartEvent(processBuilder);

    processWorkInProgress = blockBuilder.buildFlowNodes(processWorkInProgress);

    return processWorkInProgress.endEvent(endEventId).done();
  }

  public ExecutionPath findRandomExecutionPath(final Random random) {
    final var followingPath = blockBuilder.findRandomExecutionPath(random);

    final var startPath =
        startEventBuilder.findRandomExecutionPath(processId, followingPath.collectVariables());
    startPath.append(followingPath);

    if (hasEventSubProcess) {
      final var shouldTriggerEventSubProcess = true; // random.nextBoolean();
      if (shouldTriggerEventSubProcess) {
        executionPathForEventSubProcess(random, startPath);
      }
    }

    return new ExecutionPath(processId, startPath);
  }

  private void executionPathForEventSubProcess(
      final Random random,
      final io.zeebe.test.util.bpmn.random.ExecutionPathSegment followingPath) {
    final var size = followingPath.getSteps().size();

    if (size < 1) {
      // empty path
      return;
    }

    final var index = random.nextInt(size);
    if (isEventSubProcessInterrupting) {

      final var first =
          followingPath.getSteps().stream()
              .filter(
                  s ->
                      StepPublishMessage.class.isInstance(s)
                          || StepPublishStartMessage.class.isInstance(s))
              .findAny();

      if (first.isPresent()) {
        // publish for intermediate message catch event
        // concurrent publish cause problems -
        // https://github.com/camunda-cloud/zeebe/issues/6552
        return;
      }
      //      if (index > 0) {
      //        final var abstractExecutionStep = followingPath.getSteps().get(index - 1);
      //
      //        if (abstractExecutionStep instanceof StepPublishMessage) {
      //          // publish for intermediate message catch event
      //          // concurrent publish cause problems -
      //          // https://github.com/camunda-cloud/zeebe/issues/6552
      //          // we remove that as well
      //          index = index - 1;
      //        }
      //      }

      // if it is interrupting we remove the other execution path
      followingPath.replace(index, new StepPublishMessage(eventSubProcessMessageName));
    } else {
      followingPath.insert(index, new StepPublishMessage(eventSubProcessMessageName));
    }
  }
}
