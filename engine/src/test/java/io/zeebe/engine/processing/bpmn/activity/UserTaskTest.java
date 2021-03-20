/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.zeebe.engine.processing.bpmn.activity;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.Protocol;
import io.zeebe.protocol.record.Assertions;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.intent.JobIntent;
import io.zeebe.protocol.record.value.JobRecordValue;
import io.zeebe.test.util.Strings;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.test.util.record.RecordingExporterTestWatcher;
import java.util.Map;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public final class UserTaskTest {

  @ClassRule public static final EngineRule ENGINE = EngineRule.singlePartition();

  @Rule
  public final RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();

  @Test
  public void shouldCreateJobFromUserTask() {
    // given
    final String bpmnProcessId = Strings.newRandomValidBpmnId();
    final String userTaskId = Strings.newRandomValidBpmnId();
    final String userTaskFormKey = Strings.newRandomValidBpmnId();

    final BpmnModelInstance modelInstance =
        Bpmn.createExecutableProcess(bpmnProcessId)
            .startEvent()
            .userTask(userTaskId)
            .zeebeUserTaskForm(userTaskFormKey, "{\"fields\": []}")
            .endEvent()
            .done();

    final long processDefinitionKey =
        ENGINE
            .deployment()
            .withXmlResource(modelInstance)
            .deploy()
            .getValue()
            .getDeployedProcesses()
            .get(0)
            .getProcessDefinitionKey();

    // when
    final long processInstanceKey =
        ENGINE.processInstance().ofBpmnProcessId(bpmnProcessId).create();

    // then
    final Record<JobRecordValue> job =
        RecordingExporter.jobRecords(JobIntent.CREATED)
            .withProcessInstanceKey(processInstanceKey)
            .getFirst();

    Assertions.assertThat(job.getValue())
        .hasBpmnProcessId(bpmnProcessId)
        .hasProcessDefinitionKey(processDefinitionKey)
        .hasElementId(userTaskId)
        .hasRetries(1)
        .hasType(Protocol.USER_TASK_JOB_TYPE)
        .hasCustomHeaders(
            Map.of(
                Protocol.USER_TASK_FORM_KEY_HEADER_NAME, "camunda-forms:bpmn:" + userTaskFormKey));
  }

  @Test
  public void shouldPassthroughDefiniedTaskHeaders() {
    // given
    final String bpmnProcessId = Strings.newRandomValidBpmnId();
    final String userTaskId = Strings.newRandomValidBpmnId();
    final String userTaskFormKey = Strings.newRandomValidBpmnId();

    final BpmnModelInstance modelInstance =
        Bpmn.createExecutableProcess(bpmnProcessId)
            .startEvent()
            .userTask(userTaskId)
            .zeebeTaskHeader("userTaskHeader", "userTaskHeaderValue")
            .zeebeFormKey(userTaskFormKey)
            .endEvent()
            .done();

    final long processDefinitionKey =
        ENGINE
            .deployment()
            .withXmlResource(modelInstance)
            .deploy()
            .getValue()
            .getDeployedProcesses()
            .get(0)
            .getProcessDefinitionKey();

    // when
    final long processInstanceKey =
        ENGINE.processInstance().ofBpmnProcessId(bpmnProcessId).create();

    // then
    final Record<JobRecordValue> job =
        RecordingExporter.jobRecords(JobIntent.CREATED)
            .withProcessInstanceKey(processInstanceKey)
            .getFirst();

    Assertions.assertThat(job.getValue())
        .hasElementId(userTaskId)
        .hasType(Protocol.USER_TASK_JOB_TYPE)
        .hasCustomHeaders(
            Map.of(
                "userTaskHeader",
                "userTaskHeaderValue",
                Protocol.USER_TASK_FORM_KEY_HEADER_NAME,
                "camunda-forms:bpmn:" + userTaskFormKey));
  }
}
