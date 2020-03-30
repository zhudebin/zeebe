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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public final class PerfDeploymentTest {

  public static final int WARM_UP_ITERATION = 1;
  public static final int ITER_COUNT = 1;

  private static final String PROCESS_ID = "process";
  private static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess(PROCESS_ID).startEvent("startEvent").endEvent().done();

  @Parameter(0)
  public String testName;

  @Rule
  @Parameter(1)
  public EngineRule engineRule;

  @Parameters(name = "{0}")
  public static Object[][] parameters() {
    return new Object[][] {
      {"Default CFG", EngineRule.singlePartition(4 * 1024 * 1024, 128 * 1024 * 1024)},
      {"Default CFG - reduced by factor 1024", EngineRule.singlePartition(4 * 1024, 128 * 1024)},
    };
  }

  @Before
  public void setup() {
    Loggers.WORKFLOW_PROCESSOR_LOGGER.warn("Running test {}", testName);
    engineRule.deployment().withXmlResource(WORKFLOW).deploy();

    warmup();
  }

  @Test
  public void shouldCreateDeploymentWithBpmnXml() {
    var min = Long.MAX_VALUE;
    var max = Long.MIN_VALUE;
    var sum = 0L;
    var avg = 0.0f;
    Long previousExecutionTime = null;
    var sumSquareSuccessiveDifference = 0.0;
    var rMSSD = 0.0;

    for (int i = 0; i < ITER_COUNT; i++) {
      final var process = engineRule.workflowInstance().ofBpmnProcessId("process").create();

      final long startTime = getStartTime(process);
      final long endtime = getEndtime(process);
      final var executionTime = calculateMs(endtime - startTime);

      if (executionTime < min) {
        min = executionTime;
      }
      if (executionTime > max) {
        max = executionTime;
      }

      sum += executionTime;

      // approximate standard deviation by root mean square successive differences (rMSSD)
      // see:
      // https://support.minitab.com/en-us/minitab/18/help-and-how-to/quality-and-process-improvement/control-charts/supporting-topics/estimating-variation/individuals-data/#mssd
      if (previousExecutionTime != null) {
        final double squareSuccessiveDifference =
            Math.pow(executionTime - previousExecutionTime, 2);
        sumSquareSuccessiveDifference += squareSuccessiveDifference;
      }
      previousExecutionTime = executionTime;

      if (i % 50 == 0) {
        rMSSD = Math.sqrt(sumSquareSuccessiveDifference / (2 * i)); // here i = n - 1
        avg = ((float) sum / (float) i + 1);
        Loggers.STREAM_PROCESSING.warn(
            "I: {} Execution time min: {}, max: {}, avg: {}, rMSSD: {}",
            i,
            min,
            max,
            String.format("%.3f", avg),
            String.format("%.3f", rMSSD));
      }
    }

    rMSSD = Math.sqrt(sumSquareSuccessiveDifference / (2 * ITER_COUNT));
    avg = ((float) sum / (float) ITER_COUNT);
    Loggers.STREAM_PROCESSING.warn(
        "I: {} Execution time min: {}, max: {}, avg: {}, rMSSD: {}",
        ITER_COUNT,
        min,
        max,
        String.format("%.2f", avg),
        String.format("%.3f", rMSSD));
  }

  private void warmup() {
    Loggers.STREAM_PROCESSING.warn("Warm up: ");
    final var start = System.nanoTime();
    for (int i = 0; i < WARM_UP_ITERATION; i++) {
      engineRule.workflowInstance().ofBpmnProcessId("process").create();
    }
    final var end = System.nanoTime();
    Loggers.STREAM_PROCESSING.warn("Warm up done, took {}", end - start);
  }

  private long getStartTime(final long process) {
    return RecordingExporter.workflowInstanceRecords()
        .withWorkflowInstanceKey(process)
        .withIntent(WorkflowInstanceIntent.ELEMENT_ACTIVATING)
        .withElementId("startEvent")
        .findFirst()
        .orElseThrow()
        .getTimestamp();
  }

  private long getEndtime(final long process) {
    return RecordingExporter.workflowInstanceRecords()
        .withWorkflowInstanceKey(process)
        .withIntent(WorkflowInstanceIntent.ELEMENT_ACTIVATED)
        .withElementId("startEvent")
        .findFirst()
        .orElseThrow()
        .getTimestamp();
  }

  private long calculateMs(final long nanoTime) {
    return nanoTime / 1_000_000;
  }
}
