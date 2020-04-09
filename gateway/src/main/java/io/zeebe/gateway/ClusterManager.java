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
import io.zeebe.gateway.protocol.GatewayOuterClass.ClusterJoinResponse;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClusterManager {

  private final BrokerTopologyManager topologyManager;
  private final ClusterJoinResponse response = ClusterJoinResponse.newBuilder().build();
  private final BrokerClient client;

  public ClusterManager(final BrokerClient client) {
    this.client = client;
    this.topologyManager = client.getTopologyManager();
  }

  public void updateClusterSize(
      final int newSize, final StreamObserver<ClusterJoinResponse> responseObserver) {
    Loggers.GATEWAY_LOGGER.info("Received cluster update request {}", newSize);

    if (topologyManager.getTopology() == null) {
      // Can;t do anything
    }
    final int currentSize = topologyManager.getTopology().getClusterSize();
    final Set<String> newMembers =
        IntStream.range(currentSize, newSize).mapToObj(String::valueOf).collect(Collectors.toSet());
    updateJoin(0, newSize, newMembers, responseObserver);
  }

  private void updateJoin(
      final int nodeId,
      final int clusterSize,
      final Set<String> newMembers,
      final StreamObserver<ClusterJoinResponse> responseObserver) {
    if (nodeId < clusterSize) {
      Loggers.GATEWAY_LOGGER.info("Send join request to {}", nodeId);
      client
          .sendClusterRequest(
              "reconfigure-join-partitions", newMembers, MemberId.from(String.valueOf(nodeId)))
          .whenComplete(
              (r, e) -> {
                if (e == null) {
                  updateJoin(nodeId + 1, clusterSize, newMembers, responseObserver);
                } else {
                  e.printStackTrace();
                }
              });
    } else {
      updateLeave(0, clusterSize, newMembers, responseObserver);
    }
  }

  private void updateLeave(
      final int nodeId,
      final int clusterSize,
      final Set<String> newMembers,
      final StreamObserver<ClusterJoinResponse> responseObserver) {
    if (nodeId < clusterSize) {
      Loggers.GATEWAY_LOGGER.info("Send leave request to {}", nodeId);
      client
          .sendClusterRequest(
              "reconfigure-leave-partitions", newMembers, MemberId.from(String.valueOf(nodeId)))
          .whenComplete(
              (r, e) -> {
                if (e == null) {
                  updateLeave(nodeId + 1, clusterSize, newMembers, responseObserver);
                } else {
                  e.printStackTrace();
                }
              });
    } else {
      Loggers.GATEWAY_LOGGER.info("Reconfigure complete");
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }
}
