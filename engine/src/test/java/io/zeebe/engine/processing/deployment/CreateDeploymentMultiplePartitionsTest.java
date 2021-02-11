/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.deployment;

import static io.zeebe.protocol.Protocol.DEPLOYMENT_PARTITION;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.record.Assertions;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.RecordType;
import io.zeebe.protocol.record.intent.DeploymentIntent;
import io.zeebe.protocol.record.value.DeploymentRecordValue;
import io.zeebe.protocol.record.value.deployment.DeployedProcess;
import io.zeebe.protocol.record.value.deployment.DeploymentResource;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.test.util.record.RecordingExporterTestWatcher;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public final class CreateDeploymentMultiplePartitionsTest {

  public static final String PROCESS_ID = "process";
  public static final int PARTITION_ID = DEPLOYMENT_PARTITION;
  public static final int PARTITION_COUNT = 3;
  @ClassRule public static final EngineRule ENGINE = EngineRule.multiplePartition(PARTITION_COUNT);
  private static final BpmnModelInstance PROCESS =
      Bpmn.createExecutableProcess(PROCESS_ID).startEvent().endEvent().done();
  private static final BpmnModelInstance PROCESS_2 =
      Bpmn.createExecutableProcess("process2").startEvent().endEvent().done();

  @Rule
  public final RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();

  @Test
  public void shouldCreateDeploymentOnAllPartitions() {
    // when
    final BpmnModelInstance modelInstance =
        Bpmn.createExecutableProcess("shouldCreateDeploymentOnAllPartitions")
            .startEvent()
            .endEvent()
            .done();
    final Record<DeploymentRecordValue> deployment =
        ENGINE.deployment().withXmlResource("process.bpmn", modelInstance).deploy();

    // then
    assertThat(deployment.getKey()).isGreaterThanOrEqualTo(0L);

    assertThat(deployment.getPartitionId()).isEqualTo(PARTITION_ID);
    assertThat(deployment.getRecordType()).isEqualTo(RecordType.EVENT);
    assertThat(deployment.getIntent()).isEqualTo(DeploymentIntent.DISTRIBUTED);

    ENGINE
        .getPartitionIds()
        .forEach(
            partitionId ->
                assertCreatedDeploymentEventResources(
                    partitionId,
                    deployment.getKey(),
                    (createdDeployment) -> {
                      final DeploymentResource resource =
                          createdDeployment.getValue().getResources().get(0);

                      Assertions.assertThat(resource).hasResource(bpmnXml(PROCESS));

                      final List<DeployedProcess> deployedProcesss =
                          createdDeployment.getValue().getDeployedProcesss();

                      assertThat(deployedProcesss).hasSize(1);
                      Assertions.assertThat(deployedProcesss.get(0))
                          .hasBpmnProcessId("shouldCreateDeploymentOnAllPartitions")
                          .hasVersion(1)
                          .hasProcessKey(getDeployedProcess(deployment, 0).getProcessKey())
                          .hasResourceName("process.bpmn");
                    }));
  }

  @Test
  public void shouldOnlyDistributeFromDeploymentPartition() {
    // when
    final long deploymentKey1 = ENGINE.deployment().withXmlResource(PROCESS).deploy().getKey();

    // then
    final List<Record<DeploymentRecordValue>> deploymentRecords =
        RecordingExporter.deploymentRecords()
            .withRecordKey(deploymentKey1)
            .limit(r -> r.getIntent() == DeploymentIntent.DISTRIBUTED)
            .withIntent(DeploymentIntent.DISTRIBUTE)
            .asList();

    assertThat(deploymentRecords).hasSize(1);
    assertThat(deploymentRecords.get(0).getPartitionId()).isEqualTo(DEPLOYMENT_PARTITION);
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
    assertThat(deployment.getRecordType()).isEqualTo(RecordType.EVENT);
    assertThat(deployment.getIntent()).isEqualTo(DeploymentIntent.DISTRIBUTED);

    final List<Record<DeploymentRecordValue>> createdDeployments =
        RecordingExporter.deploymentRecords()
            .withIntent(DeploymentIntent.CREATED)
            .withRecordKey(deployment.getKey())
            .limit(PARTITION_COUNT)
            .asList();

    assertThat(createdDeployments)
        .hasSize(PARTITION_COUNT)
        .extracting(Record::getValue)
        .flatExtracting(DeploymentRecordValue::getDeployedProcesss)
        .extracting(DeployedProcess::getBpmnProcessId)
        .containsOnly("process", "process2");
  }

  @Test
  public void shouldIncrementProcessVersions() {
    // given
    final BpmnModelInstance modelInstance =
        Bpmn.createExecutableProcess("shouldIncrementProcessVersions")
            .startEvent()
            .endEvent()
            .done();
    final Record<DeploymentRecordValue> firstDeployment =
        ENGINE.deployment().withXmlResource("process1.bpmn", modelInstance).deploy();

    // when
    final Record<DeploymentRecordValue> secondDeployment =
        ENGINE.deployment().withXmlResource("process2.bpmn", modelInstance).deploy();

    // then
    final List<Record<DeploymentRecordValue>> firstCreatedDeployments =
        RecordingExporter.deploymentRecords()
            .withIntent(DeploymentIntent.CREATED)
            .withRecordKey(firstDeployment.getKey())
            .limit(PARTITION_COUNT)
            .asList();

    assertThat(firstCreatedDeployments)
        .hasSize(PARTITION_COUNT)
        .extracting(Record::getValue)
        .flatExtracting(DeploymentRecordValue::getDeployedProcesss)
        .extracting(DeployedProcess::getVersion)
        .containsOnly(1);

    final List<Record<DeploymentRecordValue>> secondCreatedDeployments =
        RecordingExporter.deploymentRecords()
            .withIntent(DeploymentIntent.CREATED)
            .withRecordKey(secondDeployment.getKey())
            .limit(PARTITION_COUNT)
            .asList();

    assertThat(secondCreatedDeployments)
        .hasSize(PARTITION_COUNT)
        .extracting(Record::getValue)
        .flatExtracting(DeploymentRecordValue::getDeployedProcesss)
        .extracting(DeployedProcess::getVersion)
        .containsOnly(2);
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

    assertThat(
            RecordingExporter.deploymentRecords(DeploymentIntent.CREATE)
                .withRecordKey(repeated.getKey())
                .limit(PARTITION_COUNT - 1)
                .count())
        .isEqualTo(PARTITION_COUNT - 1);

    final List<DeployedProcess> repeatedWfs =
        RecordingExporter.deploymentRecords(DeploymentIntent.CREATED)
            .withRecordKey(repeated.getKey())
            .limit(PARTITION_COUNT)
            .map(r -> r.getValue().getDeployedProcesss().get(0))
            .collect(Collectors.toList());

    assertThat(repeatedWfs.size()).isEqualTo(PARTITION_COUNT);
    repeatedWfs.forEach(repeatedWf -> assertSameResource(originalProcesss.get(0), repeatedWf));
  }

  @Test
  public void shouldNotFilterDifferentProcesss() {
    // given
    final Record<DeploymentRecordValue> original =
        ENGINE.deployment().withXmlResource("process.bpmn", PROCESS).deploy();

    // when
    final BpmnModelInstance sameBpmnIdModel =
        Bpmn.createExecutableProcess(PROCESS_ID).startEvent().endEvent().done();
    final Record<DeploymentRecordValue> repeated =
        ENGINE.deployment().withXmlResource("process.bpmn", sameBpmnIdModel).deploy();

    // then
    final List<DeployedProcess> originalProcesss = original.getValue().getDeployedProcesss();
    final List<DeployedProcess> repeatedProcesss = repeated.getValue().getDeployedProcesss();
    assertThat(repeatedProcesss.size()).isEqualTo(originalProcesss.size()).isOne();

    assertDifferentResources(originalProcesss.get(0), repeatedProcesss.get(0));

    assertThat(
            RecordingExporter.deploymentRecords(DeploymentIntent.CREATE)
                .withRecordKey(repeated.getKey())
                .limit(PARTITION_COUNT - 1)
                .count())
        .isEqualTo(PARTITION_COUNT - 1);

    final List<DeployedProcess> repeatedWfs =
        RecordingExporter.deploymentRecords(DeploymentIntent.CREATED)
            .withRecordKey(repeated.getKey())
            .limit(PARTITION_COUNT)
            .map(r -> r.getValue().getDeployedProcesss().get(0))
            .collect(Collectors.toList());

    assertThat(repeatedWfs.size()).isEqualTo(PARTITION_COUNT);
    repeatedWfs.forEach(
        repeatedWf -> assertDifferentResources(originalProcesss.get(0), repeatedWf));
  }

  private void assertSameResource(
      final DeployedProcess original, final DeployedProcess repeated) {
    Assertions.assertThat(repeated)
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

  private byte[] bpmnXml(final BpmnModelInstance definition) {
    final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    Bpmn.writeModelToStream(outStream, definition);
    return outStream.toByteArray();
  }

  @SuppressWarnings("unchecked")
  private DeployedProcess getDeployedProcess(
      final Record<DeploymentRecordValue> record, final int offset) {
    return record.getValue().getDeployedProcesss().get(offset);
  }

  private void assertCreatedDeploymentEventResources(
      final int expectedPartition,
      final long expectedKey,
      final Consumer<Record<DeploymentRecordValue>> deploymentAssert) {
    final Record deploymentCreatedEvent =
        RecordingExporter.deploymentRecords()
            .withPartitionId(expectedPartition)
            .withIntent(DeploymentIntent.CREATED)
            .withRecordKey(expectedKey)
            .getFirst();

    assertThat(deploymentCreatedEvent.getKey()).isEqualTo(expectedKey);
    assertThat(deploymentCreatedEvent.getPartitionId()).isEqualTo(expectedPartition);

    deploymentAssert.accept(deploymentCreatedEvent);
  }
}
