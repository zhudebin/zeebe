/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.bpmn;

import io.camunda.zeebe.engine.Loggers;
import io.camunda.zeebe.engine.api.TypedRecord;
import io.camunda.zeebe.engine.metrics.JobMetrics;
import io.camunda.zeebe.engine.metrics.ProcessEngineMetrics;
import io.camunda.zeebe.engine.processing.bpmn.behavior.BpmnBehaviorsImpl;
import io.camunda.zeebe.engine.processing.bpmn.behavior.BpmnIncidentBehavior;
import io.camunda.zeebe.engine.processing.bpmn.behavior.BpmnStateTransitionBehavior;
import io.camunda.zeebe.engine.processing.common.CatchEventBehavior;
import io.camunda.zeebe.engine.processing.common.EventTriggerBehavior;
import io.camunda.zeebe.engine.processing.common.ExpressionProcessor;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableFlowElement;
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecordProcessor;
import io.camunda.zeebe.engine.processing.streamprocessor.sideeffect.SideEffectProducer;
import io.camunda.zeebe.engine.processing.streamprocessor.sideeffect.SideEffectQueue;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.LegacyTypedResponseWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.LegacyTypedStreamWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedRejectionWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.Writers;
import io.camunda.zeebe.engine.processing.variable.VariableBehavior;
import io.camunda.zeebe.engine.state.immutable.ProcessState;
import io.camunda.zeebe.engine.state.mutable.MutableZeebeState;
import io.camunda.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceRecord;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import java.util.function.Consumer;
import org.slf4j.Logger;

public final class BpmnStreamProcessor implements TypedRecordProcessor<ProcessInstanceRecord> {

  private static final Logger LOGGER = Loggers.PROCESS_PROCESSOR_LOGGER;

  private final SideEffectQueue sideEffectQueue = new SideEffectQueue();
  private final BpmnElementContextImpl context = new BpmnElementContextImpl();

  private final ProcessState processState;
  private final BpmnElementProcessors processors;
  private final ProcessInstanceStateTransitionGuard stateTransitionGuard;
  private final BpmnStateTransitionBehavior stateTransitionBehavior;
  private final TypedRejectionWriter rejectionWriter;
  private final BpmnIncidentBehavior incidentBehavior;

  public BpmnStreamProcessor(
      final ExpressionProcessor expressionProcessor,
      final CatchEventBehavior catchEventBehavior,
      final VariableBehavior variableBehavior,
      final EventTriggerBehavior eventTriggerBehavior,
      final MutableZeebeState zeebeState,
      final Writers writers,
      final JobMetrics jobMetrics,
      final ProcessEngineMetrics processEngineMetrics) {
    processState = zeebeState.getProcessState();

    final var bpmnBehaviors =
        new BpmnBehaviorsImpl(
            expressionProcessor,
            sideEffectQueue,
            zeebeState,
            catchEventBehavior,
            variableBehavior,
            eventTriggerBehavior,
            this::getContainerProcessor,
            writers,
            jobMetrics,
            processEngineMetrics);
    rejectionWriter = writers.rejection();
    incidentBehavior = bpmnBehaviors.incidentBehavior();
    processors = new BpmnElementProcessors(bpmnBehaviors);

    stateTransitionGuard = bpmnBehaviors.stateTransitionGuard();
    stateTransitionBehavior = bpmnBehaviors.stateTransitionBehavior();
  }

  private BpmnElementContainerProcessor<ExecutableFlowElement> getContainerProcessor(
      final BpmnElementType elementType) {
    return processors.getContainerProcessor(elementType);
  }

  @Override
  public void processRecord(
      final TypedRecord<ProcessInstanceRecord> record,
      final LegacyTypedResponseWriter responseWriter,
      final LegacyTypedStreamWriter streamWriter,
      final Consumer<SideEffectProducer> sideEffect) {

    // initialize
    sideEffectQueue.clear();
    sideEffect.accept(sideEffectQueue);

    final var intent = (ProcessInstanceIntent) record.getIntent();
    final var recordValue = record.getValue();

    context.init(record.getKey(), recordValue, intent);

    final var bpmnElementType = recordValue.getBpmnElementType();
    final var processor = processors.getProcessor(bpmnElementType);
    final ExecutableFlowElement element = getElement(recordValue, processor);

    stateTransitionGuard
        .isValidStateTransition(context, element)
        .ifRightOrLeft(
            ok -> {
              LOGGER.trace("Process process instance event [context: {}]", context);
              processEvent(intent, processor, element);
            },
            violation ->
                rejectionWriter.appendRejection(
                    record, RejectionType.INVALID_STATE, violation.getMessage()));
  }

  private void processEvent(
      final ProcessInstanceIntent intent,
      final BpmnElementProcessor<ExecutableFlowElement> processor,
      final ExecutableFlowElement element) {

    switch (intent) {
      case ACTIVATE_ELEMENT:
        final var activatingContext = stateTransitionBehavior.transitionToActivating(context);
        stateTransitionBehavior
            .onElementActivating(element, activatingContext)
            .ifRightOrLeft(
                ok -> processor.onActivate(element, activatingContext),
                failure -> incidentBehavior.createIncident(failure, activatingContext));
        break;
      case COMPLETE_ELEMENT:
        final var completingContext = stateTransitionBehavior.transitionToCompleting(context);
        processor.onComplete(element, completingContext);
        break;
      case TERMINATE_ELEMENT:
        final var terminatingContext = stateTransitionBehavior.transitionToTerminating(context);
        processor.onTerminate(element, terminatingContext);
        break;
      default:
        throw new BpmnProcessingException(
            context,
            String.format(
                "Expected the processor '%s' to handle the event but the intent '%s' is not supported",
                processor.getClass(), intent));
    }
  }

  private ExecutableFlowElement getElement(
      final ProcessInstanceRecord recordValue,
      final BpmnElementProcessor<ExecutableFlowElement> processor) {

    return processState.getFlowElement(
        recordValue.getProcessDefinitionKey(),
        recordValue.getElementIdBuffer(),
        processor.getType());
  }
}
