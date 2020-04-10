/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.gateway;

import io.atomix.cluster.MemberId;
import io.grpc.stub.StreamObserver;
import io.zeebe.gateway.impl.broker.BrokerClient;
import io.zeebe.gateway.impl.broker.cluster.BrokerTopologyManager;
import io.zeebe.gateway.protocol.GatewayOuterClass.UpdateClusterSizeCommitResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.UpdateClusterSizeInitResponse;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClusterManager {

  private final BrokerTopologyManager topologyManager;
  private final UpdateClusterSizeInitResponse responseInit =
      UpdateClusterSizeInitResponse.newBuilder().build();
  private final UpdateClusterSizeCommitResponse responseCommit =
      UpdateClusterSizeCommitResponse.newBuilder().build();
  private final BrokerClient client;

  public ClusterManager(final BrokerClient client) {
    this.client = client;
    this.topologyManager = client.getTopologyManager();
  }

  public void updateClusterSizeInit(
      final int newClusterSize,
      final int nodeId,
      final StreamObserver<UpdateClusterSizeInitResponse> responseObserver) {
    if (topologyManager.getTopology() == null) {
      // Can;t do anything
      responseObserver.onError(new RuntimeException("Cannot find current clustersize"));
    }
    final int currentSize = topologyManager.getTopology().getClusterSize();
    final Set<String> newMembers =
        IntStream.range(currentSize, newClusterSize)
            .mapToObj(String::valueOf)
            .collect(Collectors.toSet());

    Loggers.GATEWAY_LOGGER.info("Send join request to {}", nodeId);
    client
        .sendClusterRequest(
            "reconfigure-join-partitions", newMembers, MemberId.from(String.valueOf(nodeId)))
        .whenComplete(
            (r, e) -> {
              if (e == null) {
                responseObserver.onNext(responseInit);
                responseObserver.onCompleted();
              } else {
                e.printStackTrace();
                responseObserver.onError(e);
              }
            });
  }

  public void updateClusterSizeCommit(
      final int newClusterSize,
      final int nodeId,
      final StreamObserver<UpdateClusterSizeCommitResponse> responseObserver) {
    if (topologyManager.getTopology() == null) {
      // Can;t do anything
      responseObserver.onError(new RuntimeException("Cannot find current clustersize"));
    }
    final int currentSize = topologyManager.getTopology().getClusterSize();
    final Set<String> newMembers =
        IntStream.range(currentSize, newClusterSize)
            .mapToObj(String::valueOf)
            .collect(Collectors.toSet());

    Loggers.GATEWAY_LOGGER.info("Send join request to {}", nodeId);
    client
        .sendClusterRequest(
            "reconfigure-leave-partitions", newMembers, MemberId.from(String.valueOf(nodeId)))
        .whenComplete(
            (r, e) -> {
              if (e == null) {
                responseObserver.onNext(responseCommit);
                responseObserver.onCompleted();
              } else {
                e.printStackTrace();
                responseObserver.onError(e);
              }
            });
  }
}
