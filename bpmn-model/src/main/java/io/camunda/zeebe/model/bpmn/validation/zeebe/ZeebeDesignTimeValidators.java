/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.camunda.zeebe.model.bpmn.validation.zeebe;

import io.camunda.zeebe.model.bpmn.impl.ZeebeConstants;
import io.camunda.zeebe.model.bpmn.instance.CallActivity;
import io.camunda.zeebe.model.bpmn.instance.MultiInstanceLoopCharacteristics;
import io.camunda.zeebe.model.bpmn.instance.ScriptTask;
import io.camunda.zeebe.model.bpmn.instance.SendTask;
import io.camunda.zeebe.model.bpmn.instance.ServiceTask;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeCalledDecision;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeCalledElement;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeFormDefinition;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeLoopCharacteristics;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeSubscription;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeTaskDefinition;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.camunda.bpm.model.xml.validation.ModelElementValidator;

public final class ZeebeDesignTimeValidators {

  public static final Collection<ModelElementValidator<?>> VALIDATORS;

  static {
    final List<ModelElementValidator<?>> validators = new ArrayList<>();
    validators.add(new ActivityValidator());
    validators.add(new BoundaryEventValidator());
    validators.add(new BusinessRuleTaskValidation());
    validators.add(
        ExtensionElementsValidator.verifyThat(CallActivity.class)
            .hasSingleExtensionElement(
                ZeebeCalledElement.class, ZeebeConstants.ELEMENT_CALLED_ELEMENT));
    validators.add(new DefinitionsValidator());
    validators.add(new EndEventValidator());
    validators.add(new EventDefinitionValidator());
    validators.add(new EventBasedGatewayValidator());
    validators.add(new ErrorEventDefinitionValidator());
    validators.add(new ExclusiveGatewayValidator());
    validators.add(new FlowElementValidator());
    validators.add(new FlowNodeValidator());
    validators.add(new IntermediateCatchEventValidator());
    validators.add(new MessageEventDefinitionValidator());
    validators.add(new MessageThrowEventValidator());
    validators.add(new MessageValidator());
    validators.add(
        ExtensionElementsValidator.verifyThat(MultiInstanceLoopCharacteristics.class)
            .hasSingleExtensionElement(
                ZeebeLoopCharacteristics.class, ZeebeConstants.ELEMENT_LOOP_CHARACTERISTICS));
    validators.add(new ProcessValidator());
    validators.add(
        ExtensionElementsValidator.verifyThat(ScriptTask.class)
            .hasSingleExtensionElement(
                ZeebeTaskDefinition.class, ZeebeConstants.ELEMENT_TASK_DEFINITION));
    validators.add(new SequenceFlowValidator());
    validators.add(
        ExtensionElementsValidator.verifyThat(SendTask.class)
            .hasSingleExtensionElement(
                ZeebeTaskDefinition.class, ZeebeConstants.ELEMENT_TASK_DEFINITION));
    validators.add(
        ExtensionElementsValidator.verifyThat(ServiceTask.class)
            .hasSingleExtensionElement(
                ZeebeTaskDefinition.class, ZeebeConstants.ELEMENT_TASK_DEFINITION));
    validators.add(new ReceiveTaskValidator());
    validators.add(new StartEventValidator());
    validators.add(new SubProcessValidator());
    validators.add(new TimerEventDefinitionValidator());
    validators.add(
        ZeebeElementValidator.verifyThat(ZeebeCalledElement.class)
            .hasNonEmptyAttribute(
                ZeebeCalledElement::getProcessId, ZeebeConstants.ATTRIBUTE_PROCESS_ID));
    validators.add(new ZeebeLoopCharacteristicsValidator());
    validators.add(
        ZeebeElementValidator.verifyThat(ZeebeTaskDefinition.class)
            .hasNonEmptyAttribute(ZeebeTaskDefinition::getType, ZeebeConstants.ATTRIBUTE_TYPE)
            .hasNonEmptyAttribute(
                ZeebeTaskDefinition::getRetries, ZeebeConstants.ATTRIBUTE_RETRIES));
    validators.add(
        ZeebeElementValidator.verifyThat(ZeebeSubscription.class)
            .hasNonEmptyAttribute(
                ZeebeSubscription::getCorrelationKey, ZeebeConstants.ATTRIBUTE_CORRELATION_KEY));
    validators.add(
        ZeebeElementValidator.verifyThat(ZeebeFormDefinition.class)
            .hasNonEmptyAttribute(
                ZeebeFormDefinition::getFormKey, ZeebeConstants.ATTRIBUTE_FORM_KEY));
    validators.add(new ZeebeUserTaskFormValidator());
    validators.add(
        ZeebeElementValidator.verifyThat(ZeebeCalledDecision.class)
            .hasNonEmptyAttribute(
                ZeebeCalledDecision::getDecisionId, ZeebeConstants.ATTRIBUTE_DECISION_ID)
            .hasNonEmptyAttribute(
                ZeebeCalledDecision::getResultVariable, ZeebeConstants.ATTRIBUTE_RESULT_VARIABLE));

    VALIDATORS = Collections.unmodifiableList(validators);
  }

  private ZeebeDesignTimeValidators() {}
}
