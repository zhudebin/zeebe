/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.transport.backpressure;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.limit.Gradient2Limit;
import com.netflix.concurrency.limits.limit.GradientLimit;
import com.netflix.concurrency.limits.limit.VegasLimit;
import com.netflix.concurrency.limits.limit.WindowedLimit;
import io.zeebe.broker.system.configuration.BackpressureCfg;
import io.zeebe.broker.system.configuration.BackpressureCfg.LimitAlgorithm;
import io.zeebe.protocol.record.intent.Intent;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/** A request limiter that manages the limits for each partition independently. */
public class PartitionAwareRequestLimiter {

  private final Map<Integer, RequestLimiter<Intent>> partitionLimiters = new ConcurrentHashMap<>();

  private final Function<Integer, RequestLimiter<Intent>> limiterSupplier;

  private PartitionAwareRequestLimiter() {
    this.limiterSupplier = i -> new NoopRequestLimiter<>();
  }

  public PartitionAwareRequestLimiter(final Supplier<Limit> limitSupplier) {
    this.limiterSupplier = i -> CommandRateLimiter.builder().limit(limitSupplier.get()).build(i);
  }

  public static PartitionAwareRequestLimiter newNoopLimiter() {
    return new PartitionAwareRequestLimiter();
  }

  public static PartitionAwareRequestLimiter newLimiter(
      LimitAlgorithm algorithm, boolean useWindowed, BackpressureCfg backpressure) {
    final Supplier<Limit> limit;
    if (algorithm == LimitAlgorithm.GRADIENT) {
      limit = GradientLimit::newDefault;
    } else if (algorithm == LimitAlgorithm.GRADIENT2) {
      limit = Gradient2Limit::newDefault;
    } else {
      limit =
          () ->
              VegasLimit.newBuilder()
                  /*.alpha(i -> Math.max(3, (int) (i * 0.2)))
                  .beta(i -> Math.max(6, (int) (i * 0.4)))
                  .threshold(i -> 3)
                  .increase(i -> i + 3)*/
                  .build();
    }

    if (useWindowed) {
      return new PartitionAwareRequestLimiter(
          () ->
              WindowedLimit.newBuilder()
                  .maxWindowTime(backpressure.getMinWindowTime(), TimeUnit.MILLISECONDS)
                  .maxWindowTime(backpressure.getMaxWindowTime(), TimeUnit.MILLISECONDS)
                  .minRttThreshold(backpressure.getMinRttThreshold(), TimeUnit.MILLISECONDS)
                  .build(limit.get()));
    } else {
      return new PartitionAwareRequestLimiter(limit);
    }
  }

  public boolean tryAcquire(int partitionId, int streamId, long requestId, Intent context) {
    final RequestLimiter<Intent> limiter = getLimiter(partitionId);
    return limiter.tryAcquire(streamId, requestId, context);
  }

  public void onResponse(int partitionId, int streamId, long requestId) {
    final RequestLimiter limiter = partitionLimiters.get(partitionId);
    if (limiter != null) {
      limiter.onResponse(streamId, requestId);
    }
  }

  public void addPartition(int partitionId) {
    getOrCreateLimiter(partitionId);
  }

  public void removePartition(int partitionId) {
    partitionLimiters.remove(partitionId);
  }

  public RequestLimiter<Intent> getLimiter(int partitionId) {
    return getOrCreateLimiter(partitionId);
  }

  private RequestLimiter<Intent> getOrCreateLimiter(int partitionId) {
    return partitionLimiters.computeIfAbsent(partitionId, limiterSupplier::apply);
  }
}
