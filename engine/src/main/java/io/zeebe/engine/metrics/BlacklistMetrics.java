/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.metrics;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import java.util.concurrent.Callable;

public class BlacklistMetrics {

  private static final Histogram BLACKLIST_CHECK_LATENCY =
      Histogram.build()
          .namespace("zeebe")
          .name("blacklist_check_latency")
          .help("The execution time of the blacklist check")
          .labelNames("partition")
          .register();

  private static final Counter BLACKLIST_COUNT =
      Counter.build()
          .namespace("zeebe")
          .name("blacklist_count_total")
          .help("Number of blacklisted instances")
          .labelNames("partition")
          .register();

  private final String partitionIdLabel;

  public BlacklistMetrics(final int partitionId) {
    this.partitionIdLabel = String.valueOf(partitionId);
  }

  public boolean observeBlacklistCheck(final Callable<Boolean> blacklistCheck) {
    return BLACKLIST_CHECK_LATENCY.labels(partitionIdLabel).time(blacklistCheck);
  }

  public void countBlacklisting() {
    BLACKLIST_COUNT.inc();
  }
}
