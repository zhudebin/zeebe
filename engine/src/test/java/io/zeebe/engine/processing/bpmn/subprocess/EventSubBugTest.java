package io.zeebe.engine.processing.bpmn.subprocess;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.model.bpmn.builder.ProcessBuilder;
import io.zeebe.protocol.record.intent.MessageSubscriptionIntent;
import io.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.zeebe.protocol.record.value.BpmnElementType;
import io.zeebe.test.util.record.RecordingExporter;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class EventSubBugTest {

  @Rule public final EngineRule engineRule = EngineRule.singlePartition();
  private static final String PROCESS_ID = "proc";
  private static final String JOB_TYPE = "type";

  private static final String messageName = "messageName";

  @Test
  public void shouldEndProcess() {
    // given
    final ProcessBuilder process = Bpmn.createExecutableProcess(PROCESS_ID);

    process
        .eventSubProcess("event_sub_proc")
        .startEvent("event_sub_start")
        .interrupting(true)
        .message(b -> b.name(messageName).zeebeCorrelationKeyExpression("key"))
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
            .processInstance()
            .ofBpmnProcessId(PROCESS_ID)
            .withVariables(Map.of("key", 123))
            .create();

    // when
    RecordingExporter.messageSubscriptionRecords(MessageSubscriptionIntent.CREATED)
        .withProcessInstanceKey(wfInstanceKey)
        .withMessageName(messageName)
        .await();
    RecordingExporter.messageSubscriptionRecords(MessageSubscriptionIntent.CREATED)
        .withProcessInstanceKey(wfInstanceKey)
        .withMessageName("msg")
        .await();
    engineRule.message().withName("msg").withCorrelationKey("123").publish();

    RecordingExporter.processInstanceRecords()
        .withElementType(BpmnElementType.INTERMEDIATE_CATCH_EVENT)
        .withIntent(ProcessInstanceIntent.ELEMENT_ACTIVATING)
        .await();
    engineRule
        .message()
        .withName(messageName)
        .withCorrelationKey("123")
        .withVariables(Map.of("key", "123"))
        .publish();

    // then
    assertThat(
            RecordingExporter.processInstanceRecords()
                .withProcessInstanceKey(wfInstanceKey)
                .limitToProcessInstanceCompleted())
        .extracting(r -> tuple(r.getValue().getBpmnElementType(), r.getIntent()))
        .containsSubsequence(
            tuple(BpmnElementType.START_EVENT, ProcessInstanceIntent.EVENT_OCCURRED),
            tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_TERMINATED),
            tuple(BpmnElementType.SUB_PROCESS, ProcessInstanceIntent.ELEMENT_ACTIVATED),
            tuple(BpmnElementType.SUB_PROCESS, ProcessInstanceIntent.ELEMENT_COMPLETED),
            tuple(BpmnElementType.PROCESS, ProcessInstanceIntent.ELEMENT_COMPLETED));
  }
}
