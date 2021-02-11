/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.processinstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.zeebe.el.ExpressionLanguageFactory;
import io.zeebe.engine.processing.CommandProcessorTestCase;
import io.zeebe.engine.processing.common.ExpressionProcessor;
import io.zeebe.engine.processing.common.ExpressionProcessor.VariablesLookup;
import io.zeebe.engine.processing.deployment.transform.DeploymentTransformer;
import io.zeebe.engine.processing.streamprocessor.TypedRecord;
import io.zeebe.engine.state.KeyGenerator;
import io.zeebe.engine.state.ZeebeState;
import io.zeebe.engine.state.deployment.DeployedProcess;
import io.zeebe.engine.state.instance.ElementInstance;
import io.zeebe.engine.state.mutable.MutableElementInstanceState;
import io.zeebe.engine.state.mutable.MutableVariableState;
import io.zeebe.engine.state.mutable.MutableProcessState;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.impl.record.value.deployment.DeploymentRecord;
import io.zeebe.protocol.impl.record.value.deployment.Process;
import io.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceCreationRecord;
import io.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceRecord;
import io.zeebe.protocol.record.RejectionType;
import io.zeebe.protocol.record.intent.ProcessInstanceCreationIntent;
import io.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.zeebe.protocol.record.value.BpmnElementType;
import io.zeebe.test.util.MsgPackUtil;
import io.zeebe.test.util.collection.Maps;
import io.zeebe.util.buffer.BufferUtil;
import java.io.ByteArrayOutputStream;
import java.util.Map;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public final class CreateProcessInstanceProcessorTest
    extends CommandProcessorTestCase<ProcessInstanceCreationRecord> {

  private static final BpmnModelInstance VALID_PROCESS =
      Bpmn.createExecutableProcess().startEvent().endEvent().done();
  private static DeploymentTransformer transformer;

  private ZeebeState state;
  private KeyGenerator keyGenerator;
  private MutableProcessState processState;
  private MutableElementInstanceState elementInstanceState;
  private MutableVariableState variablesState;
  private CreateProcessInstanceProcessor processor;

  @BeforeClass
  public static void init() {
    final VariablesLookup emptyLookup = (variable, scopeKey) -> null;
    final var expressionProcessor =
        new ExpressionProcessor(ExpressionLanguageFactory.createExpressionLanguage(), emptyLookup);
    transformer =
        new DeploymentTransformer(
            CommandProcessorTestCase.ZEEBE_STATE_RULE.getZeebeState(), expressionProcessor);
  }

  @Before
  public void setUp() {
    state = CommandProcessorTestCase.ZEEBE_STATE_RULE.getZeebeState();
    keyGenerator = state.getKeyGenerator();
    processState = state.getProcessState();
    elementInstanceState = state.getElementInstanceState();
    variablesState = state.getVariableState();

    processor =
        new CreateProcessInstanceProcessor(
            processState, elementInstanceState, variablesState, keyGenerator);
  }

  @Test
  public void shouldRejectIfNoBpmnProcessIdOrKeyGiven() {
    // given
    final TypedRecord<ProcessInstanceCreationRecord> command =
        newCommand(ProcessInstanceCreationRecord.class);

    // when
    processor.onCommand(command, controller, streamWriter);

    // then
    refuteAccepted();
    assertRejected(RejectionType.INVALID_ARGUMENT);
  }

  @Test
  public void shouldRejectIfNoProcessFoundByKey() {
    // given
    final TypedRecord<ProcessInstanceCreationRecord> command =
        newCommand(ProcessInstanceCreationRecord.class);
    command.getValue().setProcessKey(keyGenerator.nextKey());

    // when
    processor.onCommand(command, controller, streamWriter);

    // then
    refuteAccepted();
    assertRejected(RejectionType.NOT_FOUND);
  }

  @Test
  public void shouldRejectIfNoProcessFoundByProcessId() {
    // given
    final TypedRecord<ProcessInstanceCreationRecord> command =
        newCommand(ProcessInstanceCreationRecord.class);
    command.getValue().setBpmnProcessId("process");

    // when
    processor.onCommand(command, controller, streamWriter);

    // then
    refuteAccepted();
    assertRejected(RejectionType.NOT_FOUND);
  }

  @Test
  public void shouldRejectIfNoProcessFoundByProcessIdAndVersion() {
    // given
    final TypedRecord<ProcessInstanceCreationRecord> command =
        newCommand(ProcessInstanceCreationRecord.class);
    command.getValue().setBpmnProcessId("process").setVersion(1);

    // when
    processor.onCommand(command, controller, streamWriter);

    // then
    refuteAccepted();
    assertRejected(RejectionType.NOT_FOUND);
  }

  @Test
  public void shouldRejectIfProcessHasNoNoneStartEvent() {
    // given
    final BpmnModelInstance process =
        Bpmn.createExecutableProcess()
            .startEvent()
            .message(m -> m.name("message").zeebeCorrelationKeyExpression("key"))
            .endEvent()
            .done();
    final DeployedProcess process = deployNewProcess(process);
    final TypedRecord<ProcessInstanceCreationRecord> command =
        newCommand(ProcessInstanceCreationRecord.class);
    command.getValue().setBpmnProcessId(process.getBpmnProcessId());

    // when
    processor.onCommand(command, controller, streamWriter);

    // then
    refuteAccepted();
    assertRejected(RejectionType.INVALID_STATE);
  }

  @Test
  public void shouldRejectIfVariablesIsAnInvalidDocument() {
    // given
    final DeployedProcess process = deployNewProcess();
    final TypedRecord<ProcessInstanceCreationRecord> command =
        newCommand(ProcessInstanceCreationRecord.class);
    final MutableDirectBuffer badDocument = new UnsafeBuffer(MsgPackUtil.asMsgPack("{ 'foo': 1 }"));
    command.getValue().setBpmnProcessId(process.getBpmnProcessId()).setVariables(badDocument);
    badDocument.putByte(0, (byte) 0); // overwrites map header

    // when
    processor.onCommand(command, controller, streamWriter);

    // then
    refuteAccepted();
    assertRejected(RejectionType.INVALID_ARGUMENT);
  }

  @Test
  public void shouldActivateElementInstance() {
    // given
    final DirectBuffer variables =
        MsgPackUtil.asMsgPack(Maps.of(entry("foo", "bar"), entry("baz", "boz")));
    final DeployedProcess process = deployNewProcess();
    final TypedRecord<ProcessInstanceCreationRecord> command =
        newCommand(ProcessInstanceCreationRecord.class);
    command.getValue().setBpmnProcessId(process.getBpmnProcessId()).setVariables(variables);

    // when
    processor.onCommand(command, controller, streamWriter);

    // then
    final ProcessInstanceCreationRecord acceptedRecord =
        getAcceptedRecord(ProcessInstanceCreationIntent.CREATED);
    final long instanceKey = acceptedRecord.getProcessInstanceKey();
    final ElementInstance instance = elementInstanceState.getInstance(instanceKey);
    final ProcessInstanceRecord expectedElementActivatingRecord =
        newExpectedElementActivatingRecord(process, instanceKey);
    assertThat(instance.getState()).isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATING);
    assertThat(instance.getValue()).isEqualTo(expectedElementActivatingRecord);
    verifyElementActivatingPublished(instanceKey, instance);
  }

  @Test
  public void shouldSetVariablesFromDocument() {
    // given
    final Map<String, Object> document = Maps.of(entry("foo", "bar"), entry("baz", "boz"));
    final DeployedProcess process = deployNewProcess();
    final TypedRecord<ProcessInstanceCreationRecord> command =
        newCommand(ProcessInstanceCreationRecord.class);
    command
        .getValue()
        .setBpmnProcessId(process.getBpmnProcessId())
        .setVariables(MsgPackUtil.asMsgPack(document));

    // when
    processor.onCommand(command, controller, streamWriter);

    // then
    final ProcessInstanceCreationRecord acceptedRecord =
        getAcceptedRecord(ProcessInstanceCreationIntent.CREATED);
    final long scopeKey = acceptedRecord.getProcessInstanceKey();
    MsgPackUtil.assertEquality(
        variablesState.getVariableLocal(scopeKey, BufferUtil.wrapString("foo")), "\"bar\"");
    MsgPackUtil.assertEquality(
        variablesState.getVariableLocal(scopeKey, BufferUtil.wrapString("baz")), "\"boz\"");
  }

  @Test
  public void shouldAcceptAndUpdateKey() {
    // given
    final DeployedProcess process = deployNewProcess();
    final TypedRecord<ProcessInstanceCreationRecord> command =
        newCommand(ProcessInstanceCreationRecord.class);
    command.getValue().setBpmnProcessId(process.getBpmnProcessId());

    // when
    processor.onCommand(command, controller, streamWriter);

    // then
    final ProcessInstanceCreationRecord acceptedRecord =
        getAcceptedRecord(ProcessInstanceCreationIntent.CREATED);
    assertThat(acceptedRecord.getProcessKey()).isEqualTo(process.getKey());
  }

  @Test
  public void shouldAcceptAndUpdateVersion() {
    // given
    final DeployedProcess process = deployNewProcess();
    final TypedRecord<ProcessInstanceCreationRecord> command =
        newCommand(ProcessInstanceCreationRecord.class);
    command.getValue().setBpmnProcessId(process.getBpmnProcessId());

    // when
    processor.onCommand(command, controller, streamWriter);

    // then
    final ProcessInstanceCreationRecord acceptedRecord =
        getAcceptedRecord(ProcessInstanceCreationIntent.CREATED);
    assertThat(acceptedRecord.getVersion()).isEqualTo(process.getVersion());
  }

  @Test
  public void shouldAcceptAndUpdateProcessId() {
    // given
    final DeployedProcess process = deployNewProcess();
    final TypedRecord<ProcessInstanceCreationRecord> command =
        newCommand(ProcessInstanceCreationRecord.class);
    command.getValue().setProcessKey(process.getKey());

    // when
    processor.onCommand(command, controller, streamWriter);

    // then
    final ProcessInstanceCreationRecord acceptedRecord =
        getAcceptedRecord(ProcessInstanceCreationIntent.CREATED);
    assertThat(acceptedRecord.getBpmnProcessIdBuffer()).isEqualTo(process.getBpmnProcessId());
  }

  private ProcessInstanceRecord newExpectedElementActivatingRecord(
      final DeployedProcess process, final long instanceKey) {
    return new ProcessInstanceRecord()
        .setElementId(process.getBpmnProcessId())
        .setBpmnElementType(BpmnElementType.PROCESS)
        .setProcessKey(process.getKey())
        .setFlowScopeKey(-1L)
        .setVersion(process.getVersion())
        .setProcessInstanceKey(instanceKey)
        .setBpmnProcessId(process.getBpmnProcessId());
  }

  private void verifyElementActivatingPublished(
      final long instanceKey, final ElementInstance instance) {
    verify(streamWriter, times(1))
        .appendFollowUpEvent(
            eq(instanceKey),
            eq(ProcessInstanceIntent.ELEMENT_ACTIVATING),
            eq(instance.getValue()));
  }

  private DeployedProcess deployNewProcess() {
    return deployNewProcess(VALID_PROCESS);
  }

  private DeployedProcess deployNewProcess(final BpmnModelInstance model) {
    final long key = keyGenerator.nextKey();
    final DeploymentRecord deployment = newDeployment(model);
    final Process process = deployment.processs().iterator().next();
    processState.putDeployment(deployment);

    return processState.getLatestProcessVersionByProcessId(process.getBpmnProcessIdBuffer());
  }

  private DeploymentRecord newDeployment(final BpmnModelInstance model) {
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final DeploymentRecord record = new DeploymentRecord();
    Bpmn.writeModelToStream(output, model);
    record.resources().add().setResource(output.toByteArray());

    final boolean transformed = transformer.transform(record);
    assertThat(transformed)
        .as("Failed to transform deployment: %s", transformer.getRejectionReason())
        .isTrue();

    return record;
  }
}
