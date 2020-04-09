/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.it.clustering;

import static io.zeebe.broker.it.util.ZeebeAssertHelper.assertJobCompleted;
import static io.zeebe.test.util.TestUtil.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.Broker;
import io.zeebe.broker.it.util.GrpcClientRule;
import io.zeebe.client.api.worker.JobWorker;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.Protocol;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

// FIXME: rewrite tests now that leader election is not controllable
public final class DynamicScalingTest {
  public static final String NULL_VARIABLES = null;
  public static final String JOB_TYPE = "testTask";
  private static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess("process").startEvent().endEvent().done();

  public final Timeout testTimeout = Timeout.seconds(120);
  public final ClusteringRule clusteringRule = new ClusteringRule(3, 1, 2);
  public final GrpcClientRule clientRule = new GrpcClientRule(clusteringRule);

  @Rule
  public RuleChain ruleChain =
      RuleChain.outerRule(testTimeout).around(clusteringRule).around(clientRule);

  @Test
  public void shouldReConfigurePartition() throws InterruptedException {
    // given
    final Broker broker = clusteringRule.createBroker(2);
    Thread.sleep(5000);
    broker.joinPartition(3).join();
    clusteringRule.getBroker(0).leavePartition(3).join();

    waitUntil(() -> clusteringRule.getLeaderForPartition(3).getNodeId() != 0);
    waitUntil(() -> clusteringRule.getLeaderForPartition(3).getNodeId() == 2);
  }

  @Test
  public void shouldProcessAfterReConfigurePartition() throws InterruptedException {
    // given
    final Broker broker = clusteringRule.createBroker(2);

    final List<Long> jobKeys =
        IntStream.range(1, 3)
            .mapToObj(i -> clientRule.createSingleJob(JOB_TYPE))
            .collect(Collectors.toList());

    assertThat(jobKeys.stream().map(Protocol::decodePartitionId).anyMatch(p -> p == 3)).isTrue();

    // when
    broker.joinPartition(3).join();
    clusteringRule.getBroker(0).leavePartition(3).join();

    waitUntil(() -> clusteringRule.getLeaderForPartition(3).getNodeId() != 0);
    waitUntil(() -> clusteringRule.getLeaderForPartition(3).getNodeId() == 2);

    final JobCompleter jobCompleter = new JobCompleter(jobKeys);

    // then
    jobCompleter.waitForJobCompletion();

    jobCompleter.close();
  }

  @Test
  public void shouldReConfigurePartitionByUpdate() throws InterruptedException {
    // given
    final Broker broker = clusteringRule.createBroker(2);
    Thread.sleep(5000);
    broker.addNewMembers(Set.of("2")).join();
    clusteringRule.getBroker(0).addNewMembers(Set.of("2")).join();
    clusteringRule.getBroker(1).addNewMembers(Set.of("2")).join();

    waitUntil(() -> clusteringRule.getLeaderForPartition(3).getNodeId() != 0);
    waitUntil(() -> clusteringRule.getLeaderForPartition(3).getNodeId() == 2);
  }

  class JobCompleter {
    private final JobWorker jobWorker;
    private final CountDownLatch latch;

    JobCompleter(final List<Long> jobKeys) {

      latch = new CountDownLatch(jobKeys.size());
      jobWorker =
          clientRule
              .getClient()
              .newWorker()
              .jobType(JOB_TYPE)
              .handler(
                  (client, job) -> {
                    if (jobKeys.contains(job.getKey())) {
                      client.newCompleteCommand(job.getKey()).send();
                      latch.countDown();
                    }
                  })
              .open();
    }

    void waitForJobCompletion() {
      try {
        latch.await(10, TimeUnit.SECONDS);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
      assertJobCompleted();
    }

    void close() {
      if (!jobWorker.isClosed()) {
        jobWorker.close();
      }
    }
  }
}
