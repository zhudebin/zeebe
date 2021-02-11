/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.deployment;

import io.zeebe.engine.processing.deployment.model.element.ExecutableCatchEventElement;
import io.zeebe.engine.processing.deployment.model.element.ExecutableMessage;
import io.zeebe.engine.processing.deployment.model.element.ExecutableProcess;
import io.zeebe.engine.processing.deployment.model.element.ExecutableStartEvent;
import io.zeebe.engine.processing.streamprocessor.TypedRecord;
import io.zeebe.engine.processing.streamprocessor.TypedRecordProcessor;
import io.zeebe.engine.processing.streamprocessor.writers.TypedResponseWriter;
import io.zeebe.engine.processing.streamprocessor.writers.TypedStreamWriter;
import io.zeebe.engine.state.deployment.DeployedProcess;
import io.zeebe.engine.state.mutable.MutableProcessState;
import io.zeebe.protocol.impl.record.value.deployment.DeploymentRecord;
import io.zeebe.protocol.impl.record.value.deployment.Process;
import io.zeebe.protocol.impl.record.value.message.MessageStartEventSubscriptionRecord;
import io.zeebe.protocol.record.intent.DeploymentIntent;
import io.zeebe.protocol.record.intent.MessageStartEventSubscriptionIntent;
import io.zeebe.util.buffer.BufferUtil;
import java.util.List;

public final class DeploymentCreatedProcessor implements TypedRecordProcessor<DeploymentRecord> {

  private final MutableProcessState processState;
  private final boolean isDeploymentPartition;
  private final MessageStartEventSubscriptionRecord subscriptionRecord =
      new MessageStartEventSubscriptionRecord();

  public DeploymentCreatedProcessor(
      final MutableProcessState processState, final boolean isDeploymentPartition) {
    this.processState = processState;
    this.isDeploymentPartition = isDeploymentPartition;
  }

  @Override
  public void processRecord(
      final TypedRecord<DeploymentRecord> event,
      final TypedResponseWriter responseWriter,
      final TypedStreamWriter streamWriter) {
    final DeploymentRecord deploymentEvent = event.getValue();

    if (isDeploymentPartition) {
      streamWriter.appendFollowUpCommand(
          event.getKey(), DeploymentIntent.DISTRIBUTE, deploymentEvent);
    }

    for (final Process processRecord : deploymentEvent.processs()) {
      if (isLatestProcess(processRecord)) {
        closeExistingMessageStartEventSubscriptions(processRecord, streamWriter);
        openMessageStartEventSubscriptions(processRecord, streamWriter);
      }
    }
  }

  private boolean isLatestProcess(final Process process) {
    return processState
            .getLatestProcessVersionByProcessId(process.getBpmnProcessIdBuffer())
            .getVersion()
        == process.getVersion();
  }

  private void closeExistingMessageStartEventSubscriptions(
      final Process processRecord, final TypedStreamWriter streamWriter) {
    final DeployedProcess lastMsgProcess = findLastMessageStartProcess(processRecord);
    if (lastMsgProcess == null) {
      return;
    }

    subscriptionRecord.reset();
    subscriptionRecord.setProcessKey(lastMsgProcess.getKey());
    streamWriter.appendNewCommand(MessageStartEventSubscriptionIntent.CLOSE, subscriptionRecord);
  }

  private DeployedProcess findLastMessageStartProcess(final Process processRecord) {
    for (int version = processRecord.getVersion() - 1; version > 0; --version) {
      final DeployedProcess lastMsgProcess =
          processState.getProcessByProcessIdAndVersion(
              processRecord.getBpmnProcessIdBuffer(), version);
      if (lastMsgProcess != null
          && lastMsgProcess.getProcess().getStartEvents().stream().anyMatch(e -> e.isMessage())) {
        return lastMsgProcess;
      }
    }

    return null;
  }

  private void openMessageStartEventSubscriptions(
      final Process processRecord, final TypedStreamWriter streamWriter) {
    final long processKey = processRecord.getKey();
    final DeployedProcess processDefinition = processState.getProcessByKey(processKey);
    final ExecutableProcess process = processDefinition.getProcess();
    final List<ExecutableStartEvent> startEvents = process.getStartEvents();

    // if startEvents contain message events
    for (final ExecutableCatchEventElement startEvent : startEvents) {
      if (startEvent.isMessage()) {
        final ExecutableMessage message = startEvent.getMessage();

        message
            .getMessageName()
            .map(BufferUtil::wrapString)
            .ifPresent(
                messageNameBuffer -> {
                  subscriptionRecord.reset();
                  subscriptionRecord
                      .setMessageName(messageNameBuffer)
                      .setProcessKey(processKey)
                      .setBpmnProcessId(process.getId())
                      .setStartEventId(startEvent.getId());
                  streamWriter.appendNewCommand(
                      MessageStartEventSubscriptionIntent.OPEN, subscriptionRecord);
                });
      }
    }
  }
}
