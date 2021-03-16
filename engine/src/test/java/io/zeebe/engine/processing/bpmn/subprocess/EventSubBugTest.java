/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.bpmn.subprocess;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.model.bpmn.builder.ProcessBuilder;
import io.zeebe.protocol.record.intent.MessageSubscriptionIntent;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import io.zeebe.protocol.record.value.BpmnElementType;
import io.zeebe.test.util.record.RecordingExporter;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class EventSubBugTest {

  private static final String PROCESS_ID = "proc";
  private static final String MESSAGE_NAME = "messageName";

  @Rule public final EngineRule engineRule = EngineRule.singlePartition();

  @Test
  public void shouldEndProcess() {
    // given
    final ProcessBuilder process = Bpmn.createExecutableProcess(PROCESS_ID);

    process
        .eventSubProcess("event_sub_proc")
        .startEvent("event_sub_start")
        .interrupting(true)
        .message(b -> b.name(MESSAGE_NAME).zeebeCorrelationKeyExpression("key"))
        .endEvent("event_sub_end");

    final BpmnModelInstance model =
        process
            .startEvent("start_proc")
            .intermediateCatchEvent("catch")
            .message(m -> m.name("msg").zeebeCorrelationKeyExpression("key"))
            .exclusiveGateway()
            .endEvent("end_proc")
            .done();

    LoggerFactory.getLogger(EventSubBugTest.class).warn(Bpmn.convertToString(model));

    engineRule.deployment().withXmlResource(model).deploy();

    final long wfInstanceKey =
        engineRule
            .workflowInstance()
            .ofBpmnProcessId(PROCESS_ID)
            .withVariables(Map.of("key", 123))
            .create();

    // when
    RecordingExporter.messageSubscriptionRecords(MessageSubscriptionIntent.OPENED)
        .withWorkflowInstanceKey(wfInstanceKey)
        .withMessageName(MESSAGE_NAME)
        .await();
    RecordingExporter.messageSubscriptionRecords(MessageSubscriptionIntent.OPENED)
        .withWorkflowInstanceKey(wfInstanceKey)
        .withMessageName("msg")
        .await();
    engineRule.message().withName("msg").withCorrelationKey("123").publish();

    RecordingExporter.workflowInstanceRecords()
        .withElementType(BpmnElementType.INTERMEDIATE_CATCH_EVENT)
        .withIntent(WorkflowInstanceIntent.ELEMENT_ACTIVATING)
        .await();
    engineRule
        .message()
        .withName(MESSAGE_NAME)
        .withCorrelationKey("123")
        .withVariables(Map.of("key", "123"))
        .publish();

    // then
    assertThat(
            RecordingExporter.workflowInstanceRecords()
                .withWorkflowInstanceKey(wfInstanceKey)
                .limitToWorkflowInstanceCompleted())
        .extracting(r -> tuple(r.getValue().getBpmnElementType(), r.getIntent()))
        .containsSubsequence(
            tuple(BpmnElementType.START_EVENT, WorkflowInstanceIntent.EVENT_OCCURRED),
            tuple(BpmnElementType.SUB_PROCESS, WorkflowInstanceIntent.ELEMENT_ACTIVATED),
            tuple(BpmnElementType.SUB_PROCESS, WorkflowInstanceIntent.ELEMENT_COMPLETED),
            tuple(BpmnElementType.PROCESS, WorkflowInstanceIntent.ELEMENT_COMPLETED));
  }
}
