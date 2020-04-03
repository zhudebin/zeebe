/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.metrics;

import io.prometheus.client.Histogram;
import io.zeebe.util.sched.clock.ActorClock;

public class ExecutionMetrics {

  private static final Histogram WORKFLOW_INSTANCE_EXECUTION =
      Histogram.build()
          .namespace("zeebe")
          .name("workflow_instance_execution_time")
          .help("The execution time of processing a complete workflow instance")
          .labelNames("partition")
          .register();

  private final String partitionIdLabel;

  public ExecutionMetrics(final int partitionId) {
    this.partitionIdLabel = String.valueOf(partitionId);
  }

  public void observeWorkflowInstanceExecutionTime(final long created) {
    WORKFLOW_INSTANCE_EXECUTION
        .labels(partitionIdLabel)
        .observe((ActorClock.currentTimeMillis() - created) / 1000f);
  }
}
