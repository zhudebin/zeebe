/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.it.clustering;

import static io.zeebe.test.util.TestUtil.waitUntil;

import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.zeebe.broker.Broker;
import io.zeebe.broker.it.util.GrpcClientRule;
import io.zeebe.gateway.protocol.GatewayGrpc;
import io.zeebe.gateway.protocol.GatewayGrpc.GatewayStub;
import io.zeebe.gateway.protocol.GatewayOuterClass.UpdateClusterSizeCommitRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.UpdateClusterSizeInitRequest;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

// FIXME: rewrite tests now that leader election is not controllable
public final class DynamicScalingByGatewayTest {
  public static final String NULL_VARIABLES = null;
  public static final String JOB_TYPE = "testTask";
  private static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess("process").startEvent().endEvent().done();

  public final Timeout testTimeout = Timeout.seconds(180);
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
    final var client = buildClient();

    for (int i = 0; i < 3; i++) {
      final CompletableFuture<Void> future = new CompletableFuture<>();
      client
          .withDeadlineAfter(60, TimeUnit.SECONDS)
          .updateClusterSizeInit(
              UpdateClusterSizeInitRequest.newBuilder().setNewClusterSize(3).setNodeId(i).build(),
              new TestStreamObserver<>(future));
      future.join();
    }

    for (int i = 0; i < 3; i++) {
      final CompletableFuture<Void> future = new CompletableFuture<>();
      client
          .withDeadlineAfter(60, TimeUnit.SECONDS)
          .updateClusterSizeCommit(
              UpdateClusterSizeCommitRequest.newBuilder().setNewClusterSize(3).setNodeId(i).build(),
              new TestStreamObserver<>(future));
      future.join();
    }

    waitUntil(() -> clusteringRule.getLeaderForPartition(3).getNodeId() != 0);
    waitUntil(() -> clusteringRule.getLeaderForPartition(3).getNodeId() == 2);
  }

  private GatewayStub buildClient() {
    final ManagedChannel channel =
        NettyChannelBuilder.forAddress(
                clusteringRule.getGatewayAddress().getHostName(),
                clusteringRule.getGatewayAddress().getPort())
            .usePlaintext()
            .build();
    return GatewayGrpc.newStub(channel);
  }

  private final class TestStreamObserver<V> implements StreamObserver<V> {

    final CompletableFuture<Void> future;

    private TestStreamObserver(final CompletableFuture<Void> future) {
      this.future = future;
    }

    @Override
    public void onNext(final V clusterJoinResponse) {}

    @Override
    public void onError(final Throwable throwable) {}

    @Override
    public void onCompleted() {
      future.complete(null);
    }
  }
}
