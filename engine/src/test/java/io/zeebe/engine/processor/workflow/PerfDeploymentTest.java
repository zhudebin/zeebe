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
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public final class PerfDeploymentTest {
  @ClassRule public static final EngineRule ENGINE = EngineRule.singlePartition();

  public static final int WARM_UP_ITERATION = 1_000;
  public static final int ITER_COUNT = 1_000;

  private static final String PROCESS_ID = "process";
  private static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess(PROCESS_ID).startEvent("startEvent").endEvent().done();

  @Rule
  public final RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();

  @Before
  public void setup() {
    ENGINE.deployment().withXmlResource(WORKFLOW).deploy();

    Loggers.STREAM_PROCESSING.warn("Warm up: ");
    final var start = System.nanoTime();
    for (int i = 0; i < WARM_UP_ITERATION; i++) {
      ENGINE.workflowInstance().ofBpmnProcessId("process").create();
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
    Long previousExecutionTime = null;
    var sumSquareSuccessiveDifference = 0.0;
    var rMSSD = 0.0;

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

      // approximate standard deviation by root mean square successive differences (rMSSD)
      // see: https://support.minitab.com/en-us/minitab/18/help-and-how-to/quality-and-process-improvement/control-charts/supporting-topics/estimating-variation/individuals-data/#mssd
      if (previousExecutionTime != null) {
        final double squareSuccessiveDifference = Math.pow(executionTime - previousExecutionTime, 2);
        sumSquareSuccessiveDifference += squareSuccessiveDifference;
      }
      previousExecutionTime = executionTime;

      if (i % 50 == 0) {
        rMSSD = Math.sqrt(sumSquareSuccessiveDifference / (2 * i));
        avg = (int) (sum / ITER_COUNT);
        Loggers.STREAM_PROCESSING.warn("I: {} Execution time min: {}, max: {}, avg: {}, rMSSD: {}", i, min, max, avg, rMSSD);
      }
    }

    rMSSD = Math.sqrt(sumSquareSuccessiveDifference / (2 * ITER_COUNT));
    avg = (int) (sum / ITER_COUNT);
    Loggers.STREAM_PROCESSING.warn("Execution time min: {}, max: {}, avg: {}", min, max, avg);
  }

  private long calculateMs(final long nanoTime) {
    return nanoTime / 1_000_000;
  }
}
