/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.deployment;

import static io.zeebe.protocol.Protocol.DEPLOYMENT_PARTITION;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.model.bpmn.builder.ProcessBuilder;
import io.zeebe.model.bpmn.instance.Message;
import io.zeebe.protocol.record.Assertions;
import io.zeebe.protocol.record.ExecuteCommandResponseDecoder;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.RecordType;
import io.zeebe.protocol.record.RejectionType;
import io.zeebe.protocol.record.intent.DeploymentIntent;
import io.zeebe.protocol.record.value.DeploymentRecordValue;
import io.zeebe.protocol.record.value.deployment.DeployedProcess;
import io.zeebe.protocol.record.value.deployment.DeploymentResource;
import io.zeebe.test.util.record.RecordingExporterTestWatcher;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public final class CreateDeploymentTest {

  @ClassRule public static final EngineRule ENGINE = EngineRule.singlePartition();
  private static final String PROCESS_ID = "process";
  private static final String PROCESS_ID_2 = "process2";
  private static final BpmnModelInstance PROCESS =
      Bpmn.createExecutableProcess(PROCESS_ID).startEvent().endEvent().done();
  private static final BpmnModelInstance PROCESS_2 =
      Bpmn.createExecutableProcess(PROCESS_ID_2).startEvent().endEvent().done();
  private static final BpmnModelInstance PROCESS_V2 =
      Bpmn.createExecutableProcess(PROCESS_ID).startEvent("v2").endEvent().done();
  private static final BpmnModelInstance PROCESS_2_V2 =
      Bpmn.createExecutableProcess(PROCESS_ID_2).startEvent("v2").endEvent().done();

  @Rule
  public final RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();

  @Test
  public void shouldCreateDeploymentWithBpmnXml() {
    // when
    final Record<DeploymentRecordValue> deployment =
        ENGINE.deployment().withXmlResource(PROCESS).deploy();

    // then
    assertThat(deployment.getKey()).isGreaterThanOrEqualTo(0L);

    Assertions.assertThat(deployment)
        .hasPartitionId(DEPLOYMENT_PARTITION)
        .hasRecordType(RecordType.EVENT)
        .hasIntent(DeploymentIntent.DISTRIBUTED);
  }

  @Test
  public void shouldCreateDeploymentWithProcessWhichHaveUniqueKeys() {
    // given
    final BpmnModelInstance process =
        Bpmn.createExecutableProcess("process").startEvent().endEvent().done();

    // when
    final Record<DeploymentRecordValue> deployment =
        ENGINE.deployment().withXmlResource(process).deploy();

    // then
    final long processKey = deployment.getValue().getDeployedProcesss().get(0).getProcessKey();
    final long deploymentKey = deployment.getKey();
    assertThat(processKey).isNotEqualTo(deploymentKey);
  }

  @Test
  public void shouldReturnDeployedProcessDefinitions() {
    // when
    final Record<DeploymentRecordValue> firstDeployment =
        ENGINE.deployment().withXmlResource("wf1.bpmn", PROCESS).deploy();
    final Record<DeploymentRecordValue> secondDeployment =
        ENGINE.deployment().withXmlResource("wf2.bpmn", PROCESS).deploy();

    // then
    List<DeployedProcess> deployedProcesss = firstDeployment.getValue().getDeployedProcesss();
    assertThat(deployedProcesss).hasSize(1);

    DeployedProcess deployedProcess = deployedProcesss.get(0);
    assertThat(deployedProcess.getBpmnProcessId()).isEqualTo(PROCESS_ID);
    assertThat(deployedProcess.getResourceName()).isEqualTo("wf1.bpmn");

    deployedProcesss = secondDeployment.getValue().getDeployedProcesss();
    assertThat(deployedProcesss).hasSize(1);

    deployedProcess = deployedProcesss.get(0);
    assertThat(deployedProcess.getBpmnProcessId()).isEqualTo(PROCESS_ID);
    assertThat(deployedProcess.getResourceName()).isEqualTo("wf2.bpmn");
  }

  @Test
  public void shouldCreateDeploymentResourceWithCollaboration() {
    // given
    final InputStream resourceAsStream =
        getClass().getResourceAsStream("/processs/collaboration.bpmn");
    final BpmnModelInstance modelInstance = Bpmn.readModelFromStream(resourceAsStream);

    // when
    final Record<DeploymentRecordValue> deployment =
        ENGINE.deployment().withXmlResource("collaboration.bpmn", modelInstance).deploy();

    // then
    assertThat(deployment.getValue().getDeployedProcesss())
        .extracting(DeployedProcess::getBpmnProcessId)
        .contains("process1", "process2");
  }

  @Test
  public void shouldCreateDeploymentResourceWithMultipleProcesss() {
    // given

    // when
    final Record<DeploymentRecordValue> deployment =
        ENGINE
            .deployment()
            .withXmlResource("process.bpmn", PROCESS)
            .withXmlResource("process2.bpmn", PROCESS_2)
            .deploy();

    // then
    assertThat(deployment.getValue().getDeployedProcesss())
        .extracting(DeployedProcess::getBpmnProcessId)
        .contains(PROCESS_ID, PROCESS_ID_2);

    assertThat(deployment.getValue().getResources())
        .extracting(DeploymentResource::getResourceName)
        .contains("process.bpmn", "process2.bpmn");

    assertThat(deployment.getValue().getResources())
        .extracting(DeploymentResource::getResource)
        .contains(
            Bpmn.convertToString(PROCESS).getBytes(), Bpmn.convertToString(PROCESS_2).getBytes());
  }

  @Test
  public void shouldCreateDeploymentIfUnusedInvalidMessage() {
    // given
    final BpmnModelInstance process = Bpmn.createExecutableProcess().startEvent().done();
    process.getDefinitions().addChildElement(process.newInstance(Message.class));

    // when
    final Record<DeploymentRecordValue> deployment =
        ENGINE.deployment().withXmlResource(process).deploy();

    // then
    assertThat(deployment.getIntent()).isEqualTo(DeploymentIntent.DISTRIBUTED);
  }

  @Test
  public void shouldCreateDeploymentWithMessageStartEvent() {
    // given
    final ProcessBuilder processBuilder = Bpmn.createExecutableProcess();
    final BpmnModelInstance process =
        processBuilder.startEvent().message(m -> m.name("startMessage")).endEvent().done();

    // when
    final Record<DeploymentRecordValue> deployment =
        ENGINE.deployment().withXmlResource(process).deploy();

    // then
    assertThat(deployment.getIntent()).isEqualTo(DeploymentIntent.DISTRIBUTED);
  }

  @Test
  public void shouldCreateDeploymentWithMultipleMessageStartEvent() {
    // given
    final ProcessBuilder processBuilder =
        Bpmn.createExecutableProcess("processWithMulitpleMsgStartEvent");
    processBuilder.startEvent().message(m -> m.name("startMessage1")).endEvent().done();
    final BpmnModelInstance process =
        processBuilder.startEvent().message(m -> m.name("startMessage2")).endEvent().done();

    // when
    final Record<DeploymentRecordValue> deployment =
        ENGINE.deployment().withXmlResource(process).deploy();

    // then
    assertThat(deployment.getIntent()).isEqualTo(DeploymentIntent.DISTRIBUTED);
  }

  @Test
  public void shouldRejectDeploymentIfUsedInvalidMessage() {
    // given
    final BpmnModelInstance process =
        Bpmn.createExecutableProcess().startEvent().intermediateCatchEvent("invalidmessage").done();

    // when
    final Record<DeploymentRecordValue> rejectedDeployment =
        ENGINE.deployment().withXmlResource(process).expectRejection().deploy();

    // then
    Assertions.assertThat(rejectedDeployment.getRecordType())
        .isEqualTo(RecordType.COMMAND_REJECTION);
  }

  @Test
  public void shouldRejectDeploymentIfNotValidDesignTimeAspect() throws Exception {
    // given
    final Path path = Paths.get(getClass().getResource("/processs/invalid_process.bpmn").toURI());
    final byte[] resource = Files.readAllBytes(path);

    // when
    final Record<DeploymentRecordValue> rejectedDeployment =
        ENGINE.deployment().withXmlResource(resource).expectRejection().deploy();

    // then
    Assertions.assertThat(rejectedDeployment)
        .hasKey(ExecuteCommandResponseDecoder.keyNullValue())
        .hasRecordType(RecordType.COMMAND_REJECTION)
        .hasIntent(DeploymentIntent.CREATE)
        .hasRejectionType(RejectionType.INVALID_ARGUMENT);
    assertThat(rejectedDeployment.getRejectionReason())
        .contains("ERROR: Must have at least one start event");
  }

  @Test
  public void shouldRejectDeploymentIfNotValidRuntimeAspect() throws Exception {
    // given
    final Path path =
        Paths.get(getClass().getResource("/processs/invalid_process_condition.bpmn").toURI());
    final byte[] resource = Files.readAllBytes(path);

    // when
    final Record<DeploymentRecordValue> rejectedDeployment =
        ENGINE.deployment().withXmlResource(resource).expectRejection().deploy();

    // then
    Assertions.assertThat(rejectedDeployment)
        .hasKey(ExecuteCommandResponseDecoder.keyNullValue())
        .hasRecordType(RecordType.COMMAND_REJECTION)
        .hasIntent(DeploymentIntent.CREATE)
        .hasRejectionType(RejectionType.INVALID_ARGUMENT);
    assertThat(rejectedDeployment.getRejectionReason())
        .contains("Element: flow2 > conditionExpression")
        .contains("ERROR: failed to parse expression");
  }

  @Test
  public void shouldRejectDeploymentIfOneResourceIsNotValid() throws Exception {
    // given
    final Path path1 = Paths.get(getClass().getResource("/processs/invalid_process.bpmn").toURI());
    final Path path2 = Paths.get(getClass().getResource("/processs/collaboration.bpmn").toURI());
    final byte[] resource1 = Files.readAllBytes(path1);
    final byte[] resource2 = Files.readAllBytes(path2);

    final Record<DeploymentRecordValue> rejectedDeployment =
        ENGINE
            .deployment()
            .withXmlResource(resource1)
            .withXmlResource(resource2)
            .expectRejection()
            .deploy();

    // then
    Assertions.assertThat(rejectedDeployment)
        .hasKey(ExecuteCommandResponseDecoder.keyNullValue())
        .hasRecordType(RecordType.COMMAND_REJECTION)
        .hasIntent(DeploymentIntent.CREATE)
        .hasRejectionType(RejectionType.INVALID_ARGUMENT);
  }

  @Test
  public void shouldRejectDeploymentIfNoResources() {
    // when
    final Record<DeploymentRecordValue> rejectedDeployment =
        ENGINE.deployment().expectRejection().deploy();

    // then
    Assertions.assertThat(rejectedDeployment)
        .hasKey(ExecuteCommandResponseDecoder.keyNullValue())
        .hasRecordType(RecordType.COMMAND_REJECTION)
        .hasIntent(DeploymentIntent.CREATE)
        .hasRejectionType(RejectionType.INVALID_ARGUMENT);
  }

  @Test
  public void shouldRejectDeploymentIfNotParsable() {
    // when
    final Record<DeploymentRecordValue> rejectedDeployment =
        ENGINE
            .deployment()
            .withXmlResource("not a process".getBytes(UTF_8))
            .expectRejection()
            .deploy();

    // then
    Assertions.assertThat(rejectedDeployment)
        .hasKey(ExecuteCommandResponseDecoder.keyNullValue())
        .hasRecordType(RecordType.COMMAND_REJECTION)
        .hasIntent(DeploymentIntent.CREATE)
        .hasRejectionType(RejectionType.INVALID_ARGUMENT);
  }

  @Test
  public void shouldIncrementProcessVersions() {
    // given
    final BpmnModelInstance modelInstance =
        Bpmn.createExecutableProcess("shouldIncrementProcessVersions")
            .startEvent()
            .endEvent()
            .done();

    // when
    final Record<DeploymentRecordValue> deployment =
        ENGINE.deployment().withXmlResource("process1", modelInstance).deploy();
    final Record<DeploymentRecordValue> deployment2 =
        ENGINE.deployment().withXmlResource("process2", modelInstance).deploy();

    // then
    assertThat(deployment.getValue().getDeployedProcesss().get(0).getVersion()).isEqualTo(1L);
    assertThat(deployment2.getValue().getDeployedProcesss().get(0).getVersion()).isEqualTo(2L);
  }

  @Test
  public void shouldFilterDuplicateProcess() {
    // given
    final Record<DeploymentRecordValue> original =
        ENGINE.deployment().withXmlResource("process.bpmn", PROCESS).deploy();

    // when
    final Record<DeploymentRecordValue> repeated =
        ENGINE.deployment().withXmlResource("process.bpmn", PROCESS).deploy();

    // then
    assertThat(repeated.getKey()).isGreaterThan(original.getKey());

    final List<DeployedProcess> originalProcesss = original.getValue().getDeployedProcesss();
    final List<DeployedProcess> repeatedProcesss = repeated.getValue().getDeployedProcesss();
    assertThat(repeatedProcesss.size()).isEqualTo(originalProcesss.size()).isOne();

    assertSameResource(originalProcesss.get(0), repeatedProcesss.get(0));
  }

  @Test
  public void shouldNotFilterWithDifferentResourceName() {
    // given
    final String originalResourceName = "process-1.bpmn";
    final Record<DeploymentRecordValue> original =
        ENGINE.deployment().withXmlResource(originalResourceName, PROCESS).deploy();

    // when
    final String repeatedResourceName = "process-2.bpmn";
    final Record<DeploymentRecordValue> repeated =
        ENGINE.deployment().withXmlResource(repeatedResourceName, PROCESS).deploy();

    // then
    final List<DeployedProcess> originalProcesss = original.getValue().getDeployedProcesss();
    final List<DeployedProcess> repeatedProcesss = repeated.getValue().getDeployedProcesss();
    assertThat(repeatedProcesss.size()).isEqualTo(originalProcesss.size()).isOne();

    assertDifferentResources(originalProcesss.get(0), repeatedProcesss.get(0));
    assertThat(originalProcesss.get(0).getResourceName()).isEqualTo(originalResourceName);
    assertThat(repeatedProcesss.get(0).getResourceName()).isEqualTo(repeatedResourceName);
  }

  @Test
  public void shouldNotFilterWithDifferentResource() {
    // given
    final Record<DeploymentRecordValue> original =
        ENGINE.deployment().withXmlResource("process.bpmn", PROCESS).deploy();

    // when
    final Record<DeploymentRecordValue> repeated =
        ENGINE.deployment().withXmlResource("process.bpmn", PROCESS_V2).deploy();

    // then
    final List<DeployedProcess> originalProcesss = original.getValue().getDeployedProcesss();
    final List<DeployedProcess> repeatedProcesss = repeated.getValue().getDeployedProcesss();
    assertThat(repeatedProcesss.size()).isEqualTo(originalProcesss.size()).isOne();

    assertDifferentResources(originalProcesss.get(0), repeatedProcesss.get(0));
  }

  @Test
  public void shouldFilterWithTwoEqualResources() {
    // given
    final Record<DeploymentRecordValue> original =
        ENGINE
            .deployment()
            .withXmlResource("p1.bpmn", PROCESS)
            .withXmlResource("p2.bpmn", PROCESS_2)
            .deploy();

    // when
    final Record<DeploymentRecordValue> repeated =
        ENGINE
            .deployment()
            .withXmlResource("p1.bpmn", PROCESS)
            .withXmlResource("p2.bpmn", PROCESS_2)
            .deploy();

    // then
    final List<DeployedProcess> originalProcesss = original.getValue().getDeployedProcesss();
    final List<DeployedProcess> repeatedProcesss = repeated.getValue().getDeployedProcesss();
    assertThat(repeatedProcesss.size()).isEqualTo(originalProcesss.size()).isEqualTo(2);

    for (final DeployedProcess process : originalProcesss) {
      assertSameResource(process, findProcess(repeatedProcesss, process.getBpmnProcessId()));
    }
  }

  @Test
  public void shouldFilterWithOneDifferentAndOneEqual() {
    // given
    final Record<DeploymentRecordValue> original =
        ENGINE
            .deployment()
            .withXmlResource("p1.bpmn", PROCESS)
            .withXmlResource("p2.bpmn", PROCESS_2)
            .deploy();

    // when
    final Record<DeploymentRecordValue> repeated =
        ENGINE
            .deployment()
            .withXmlResource("p1.bpmn", PROCESS)
            .withXmlResource("p2.bpmn", PROCESS_2_V2)
            .deploy();

    // then
    final List<DeployedProcess> originalProcesss = original.getValue().getDeployedProcesss();
    final List<DeployedProcess> repeatedProcesss = repeated.getValue().getDeployedProcesss();
    assertThat(repeatedProcesss.size()).isEqualTo(originalProcesss.size()).isEqualTo(2);

    assertSameResource(
        findProcess(originalProcesss, PROCESS_ID), findProcess(repeatedProcesss, PROCESS_ID));
    assertDifferentResources(
        findProcess(originalProcesss, PROCESS_ID_2), findProcess(repeatedProcesss, PROCESS_ID_2));
  }

  @Test
  public void shouldNotFilterWithRollbackToPreviousVersion() {
    // given
    final Record<DeploymentRecordValue> original =
        ENGINE.deployment().withXmlResource("p1.bpmn", PROCESS).deploy();
    ENGINE.deployment().withXmlResource("p1.bpmn", PROCESS_V2).deploy();

    // when
    final Record<DeploymentRecordValue> rollback =
        ENGINE.deployment().withXmlResource("p1.bpmn", PROCESS).deploy();

    // then
    final List<DeployedProcess> originalProcesss = original.getValue().getDeployedProcesss();
    final List<DeployedProcess> repeatedProcesss = rollback.getValue().getDeployedProcesss();
    assertThat(repeatedProcesss.size()).isEqualTo(originalProcesss.size()).isOne();

    assertDifferentResources(
        findProcess(originalProcesss, PROCESS_ID), findProcess(repeatedProcesss, PROCESS_ID));
  }

  @Test
  public void shouldRejectDeploymentWithDuplicateResources() {
    // given
    final BpmnModelInstance definition1 =
        Bpmn.createExecutableProcess("process1").startEvent().done();
    final BpmnModelInstance definition2 =
        Bpmn.createExecutableProcess("process2").startEvent().done();
    final BpmnModelInstance definition3 =
        Bpmn.createExecutableProcess("process2")
            .startEvent()
            .serviceTask("task", (t) -> t.zeebeJobType("j").zeebeTaskHeader("k", "v"))
            .done();

    // when
    final Record<DeploymentRecordValue> deploymentRejection =
        ENGINE
            .deployment()
            .withXmlResource("p1.bpmn", definition1)
            .withXmlResource("p2.bpmn", definition2)
            .withXmlResource("p3.bpmn", definition3)
            .expectRejection()
            .deploy();

    // then
    Assertions.assertThat(deploymentRejection)
        .hasRejectionType(RejectionType.INVALID_ARGUMENT)
        .hasRejectionReason(
            "Expected to deploy new resources, but encountered the following errors:\n"
                + "Duplicated process id in resources 'p2.bpmn' and 'p3.bpmn'");
  }

  @Test
  public void shouldRejectDeploymentWithInvalidTimerStartEventExpression() {
    // given
    final BpmnModelInstance definition =
        Bpmn.createExecutableProcess("process1")
            .startEvent("start-event-1")
            .timerWithCycleExpression("INVALID_CYCLE_EXPRESSION")
            .done();

    // when
    final Record<DeploymentRecordValue> deploymentRejection =
        ENGINE.deployment().withXmlResource("p1.bpmn", definition).expectRejection().deploy();

    // then
    Assertions.assertThat(deploymentRejection)
        .hasRejectionType(RejectionType.INVALID_ARGUMENT)
        .hasRejectionReason(
            "Expected to deploy new resources, but encountered the following errors:\n"
                + "'p1.bpmn': - Element: start-event-1\n"
                + "    - ERROR: Invalid timer cycle expression ("
                + "failed to evaluate expression "
                + "'INVALID_CYCLE_EXPRESSION': no variable found for name "
                + "'INVALID_CYCLE_EXPRESSION')\n");
  }

  private DeployedProcess findProcess(
      final List<DeployedProcess> processs, final String processId) {
    return processs.stream()
        .filter(w -> w.getBpmnProcessId().equals(processId))
        .findFirst()
        .orElse(null);
  }

  private void assertSameResource(final DeployedProcess original, final DeployedProcess repeated) {
    io.zeebe.protocol.record.Assertions.assertThat(repeated)
        .hasVersion(original.getVersion())
        .hasProcessKey(original.getProcessKey())
        .hasResourceName(original.getResourceName())
        .hasBpmnProcessId(original.getBpmnProcessId());
  }

  private void assertDifferentResources(
      final DeployedProcess original, final DeployedProcess repeated) {
    assertThat(original.getProcessKey()).isLessThan(repeated.getProcessKey());
    assertThat(original.getVersion()).isLessThan(repeated.getVersion());
  }
}
