/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.zeebe.engine.processing.streamprocessor;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.engine.util.ProcessExecutor;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.zeebe.protocol.record.value.BpmnElementType;
import io.zeebe.test.util.bpmn.random.ExecutionPath;
import io.zeebe.test.util.bpmn.random.TestDataGenerator;
import io.zeebe.test.util.bpmn.random.TestDataGenerator.TestDataRecord;
import io.zeebe.test.util.record.RecordingExporter;
import java.util.Collection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class ProcessExecutionRandomizedPropertyTest implements PropertyBasedTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ProcessExecutionRandomizedPropertyTest.class);
  /*
   * Some notes on scaling of these tests:
   * With 10 processes and 100 paths there is a theoretical maximum of 1000 records.
   * However, in tests the number of actual records was around 300, which can execute in about 1 m.
   *
   * Having a high number of random execution paths has only a small effect as there are rarely 100
   * different execution paths for any given process. Having a high number of paths gives us a good
   * chance to exhaust all possible paths within a given process.
   *
   * This is only true if the complexity of the processes stays constant.
   * Increasing the maximum number of blocks, depth or branches could increase the number of
   * possible paths exponentially
   */
  private static final int PROCESS_COUNT = 10;
  private static final int EXECUTION_PATH_COUNT = 100;

  @Rule public TestWatcher failedTestDataPrinter = new FailedPropertyBasedTestDataPrinter(this);
  @Rule public final EngineRule engineRule = EngineRule.singlePartition();

  @Parameter public TestDataRecord record;

  private final ProcessExecutor processExecutor = new ProcessExecutor(engineRule);

  @Override
  public TestDataRecord getDataRecord() {
    return record;
  }

  /**
   * This test takes a random process and execution path in that process. A process instance is
   * started and the process is executed according to the random execution path. The test passes if
   * it reaches the end of the process.
   */
  @Test
  public void shouldExecuteProcessToEnd() {
    final BpmnModelInstance model = record.getBpmnModel();
    LOG.error("Generated Model: {}", Bpmn.convertToString(model));
    engineRule.deployment().withXmlResource(model).deploy();

    final ExecutionPath path = record.getExecutionPath();

    path.getSteps().forEach(processExecutor::applyStep);

    // wait for the completion of the process
    RecordingExporter.processInstanceRecords(ProcessInstanceIntent.ELEMENT_COMPLETED)
        .withElementType(BpmnElementType.PROCESS)
        .withBpmnProcessId(path.getProcessId())
        .await();
  }

  @Parameters(name = "{0}")
  public static Collection<TestDataGenerator.TestDataRecord> getTestRecords() {
    return TestDataGenerator.generateTestRecords(PROCESS_COUNT, EXECUTION_PATH_COUNT);
  }
}
