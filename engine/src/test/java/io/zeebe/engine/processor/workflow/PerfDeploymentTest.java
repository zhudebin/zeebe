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
import io.zeebe.engine.util.TimeAggregation;
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

  public static final int WARM_UP_ITERATION = 1_000;
  public static final int ITER_COUNT = 1000;

  private static final String PROCESS_ID = "process";
  private static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess(PROCESS_ID).startEvent("startEvent").endEvent().done();

  @Parameter(0)
  public String testName;

  @Rule
  @Parameter(1)
  public EngineRule warmUpRule;

  @Rule
  @Parameter(2)
  public EngineRule engineRule;

  @Parameters(name = "{0}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        "Default CFG",
        // warm up
        EngineRule.singlePartition(4 * 1024 * 1024, 128 * 1024 * 1024),
        // run
        EngineRule.singlePartition(4 * 1024 * 1024, 128 * 1024 * 1024)
      },
      {
        "Default CFG - reduced by factor 1024",
        // warm up
        EngineRule.singlePartition(4 * 1024, 128 * 1024),
        // run
        EngineRule.singlePartition(4 * 1024, 128 * 1024)
      },
    };
  }

  @Before
  public void setup() {
    Loggers.WORKFLOW_PROCESSOR_LOGGER.warn("Running test {}", testName);
    warmUpRule.deployment().withXmlResource(WORKFLOW).deploy();
    engineRule.deployment().withXmlResource(WORKFLOW).deploy();

    warmup();
  }

  @Test
  public void shouldCreateDeploymentWithBpmnXml() {
    final TimeAggregation timeAggregation =
        new TimeAggregation("START_EVENT:ELEMENT_ACTIVATING", "START_EVENT:ELEMENT_ACTIVATED");
    for (int i = 0; i < ITER_COUNT; i++) {
      final var process = engineRule.workflowInstance().ofBpmnProcessId("process").create();

      timeAggregation.addNanoTime(getStartTime(process), getEndtime(process));
      // TODO timeAggregation.add(getStartTime(process), getEndtime(process));

      if ((i + 1) % 50 == 0) {
        Loggers.STREAM_PROCESSING.warn(timeAggregation.toString());
      }
      RecordingExporter.reset();
    }

    Loggers.STREAM_PROCESSING.warn(timeAggregation.toString());
  }

  private void warmup() {
    Loggers.STREAM_PROCESSING.warn("Will do warm up with {} iterations", WARM_UP_ITERATION);
    final var start = System.nanoTime();
    for (int i = 0; i < WARM_UP_ITERATION; i++) {
      warmUpRule.workflowInstance().ofBpmnProcessId("process").create();
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
}
