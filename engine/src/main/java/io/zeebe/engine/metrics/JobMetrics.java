/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.metrics;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.zeebe.util.sched.clock.ActorClock;

public final class JobMetrics {

  private static final Counter JOB_EVENTS =
      Counter.build()
          .namespace("zeebe")
          .name("job_events_total")
          .help("Number of job events")
          .labelNames("action", "partition", "type")
          .register();

  private static final Gauge PENDING_JOBS =
      Gauge.build()
          .namespace("zeebe")
          .name("pending_jobs_total")
          .help("Number of pending jobs")
          .labelNames("partition", "type")
          .register();

  private static final Histogram JOB_LIFE_TIME =
      Histogram.build()
          .namespace("zeebe")
          .name("job_life_time")
          .help("The life time of an job")
          .labelNames("partition")
          .register();

  private static final Histogram JOB_ACTIVATION_TIME =
      Histogram.build()
          .namespace("zeebe")
          .name("job_activation_time")
          .help("The time until an job was activated")
          .labelNames("partition")
          .register();

  private final String partitionIdLabel;

  public JobMetrics(final int partitionId) {
    partitionIdLabel = String.valueOf(partitionId);
  }

  private void jobEvent(final String action, final String type) {
    JOB_EVENTS.labels(action, partitionIdLabel, type).inc();
  }

  public void jobCreated(final String type) {
    jobEvent("created", type);
    PENDING_JOBS.labels(partitionIdLabel, type).inc();
  }

  private void jobFinished(final String type) {
    PENDING_JOBS.labels(partitionIdLabel, type).dec();
  }

  public void jobActivated(final String type) {
    jobEvent("activated", type);
  }

  public void jobTimedOut(final String type) {
    jobEvent("timed out", type);
  }

  public void jobCompleted(final String type) {
    jobEvent("completed", type);
    jobFinished(type);
  }

  public void jobFailed(final String type) {
    jobEvent("failed", type);
  }

  public void jobCanceled(final String type) {
    jobEvent("canceled", type);
    jobFinished(type);
  }

  public void jobErrorThrown(final String type) {
    jobEvent("error thrown", type);
    jobFinished(type);
  }

  public void observeJobLifeTime(final long creationTime) {
    JOB_LIFE_TIME
        .labels(partitionIdLabel)
        .observe((ActorClock.currentTimeMillis() - creationTime) / 1000f);
  }

  public void observeJobActivationTime(final long creationTime) {
    JOB_ACTIVATION_TIME
        .labels(partitionIdLabel)
        .observe((ActorClock.currentTimeMillis() - creationTime) / 1000f);
  }
}
