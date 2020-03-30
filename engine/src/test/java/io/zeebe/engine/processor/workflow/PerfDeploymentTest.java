/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor.workflow;

import io.zeebe.engine.Loggers;
import io.zeebe.engine.util.EngineRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.test.util.record.RecordingExporterTestWatcher;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public final class PerfDeploymentTest {
  @ClassRule public static final EngineRule ENGINE = EngineRule.singlePartition();
  private static final String PROCESS_ID = "process";
  private static final String PROCESS_ID_2 = "process2";
  private static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess(PROCESS_ID).startEvent("startEvent").endEvent().done();

  public static final int WARM_UP_ITERATION = 1_000;
  public static final int ITER_COUNT = 1_000;

  @Rule
  public final RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();

  @Before
  public void setup() {
    ENGINE.deployment().withXmlResource(WORKFLOW).deploy();

    final List<Long> keys = new ArrayList<>();
    Loggers.STREAM_PROCESSING.warn("Warm up: ");
    final var start = System.nanoTime();
    for (int i = 0; i < WARM_UP_ITERATION; i++) {
      final var process = ENGINE.workflowInstance().ofBpmnProcessId("process").create();
      keys.add(process);
    }
    final var end = System.nanoTime();
    Loggers.STREAM_PROCESSING.warn("Warm up done, took {}", end - start);
  }

  @Test
  public void shouldCreateDeploymentWithBpmnXml() {
    var min = Long.MAX_VALUE;
    var max = Long.MIN_VALUE;
    var sum = 0L;
    var avg = 0;

    for (int i = 0; i < ITER_COUNT; i++) {
      final var process = ENGINE.workflowInstance().ofBpmnProcessId("process").create();
      //
      //      final var collect =
      //          RecordingExporter.workflowInstanceRecords()
      //              .withWorkflowInstanceKey(process)
      //              .limitToWorkflowInstanceCompleted()
      //              .collect(Collectors.toList());

      final var startEventActivating =
          RecordingExporter.workflowInstanceRecords()
              .withWorkflowInstanceKey(process)
              .withIntent(WorkflowInstanceIntent.ELEMENT_ACTIVATING)
              .withElementId("startEvent")
              .findFirst()
              .orElseThrow();

      final var startEventActivated =
          RecordingExporter.workflowInstanceRecords()
              .withWorkflowInstanceKey(process)
              .withIntent(WorkflowInstanceIntent.ELEMENT_ACTIVATED)
              .withElementId("startEvent")
              .findFirst()
              .orElseThrow();

      final var executionTime =
          calculateMs(startEventActivated.getTimestamp() - startEventActivating.getTimestamp());

      if (executionTime < min) {
        min = executionTime;
      }
      if (executionTime > max) {
        max = executionTime;
      }

      sum += executionTime;

      if (i % 50 == 0) {
        Loggers.STREAM_PROCESSING.warn("I: {} Execution time min: {} max: {}", i, min, max);
      }
    }

    avg = (int) (sum / ITER_COUNT);
    Loggers.STREAM_PROCESSING.warn("Execution time min: {} max: {}, avg: {}", min, max, avg);
  }

  private long calculateMs(final long nanoTime) {
    return nanoTime / 1_000_000;
  }
}
