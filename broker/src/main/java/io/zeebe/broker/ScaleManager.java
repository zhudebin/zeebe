/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker;

import io.atomix.core.Atomix;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class ScaleManager {

  private final Atomix atomix;
  private final Broker broker;

  public ScaleManager(final Atomix atomix, final Broker broker) {
    this.atomix = atomix;
    this.broker = broker;
    init();
  }

  private void init() {
    atomix
        .getCommunicationService()
        .subscribe("reconfigure-join-partitions", this::handleReconfigureJoinRequest);
    atomix
        .getCommunicationService()
        .subscribe("reconfigure-leave-partitions", this::handleReconfigureLeaveRequest);
  }

  private CompletableFuture<Void> handleReconfigureJoinRequest(final Set<String> newMembers) {
    final CompletableFuture<Void> reconfigureComplete = new CompletableFuture<>();

    broker
        .reconfigureOnlyJoin(newMembers)
        .thenApply(r -> reconfigureComplete.complete(r))
        .exceptionally(e -> reconfigureComplete.completeExceptionally(e));
    return reconfigureComplete;
  }

  private CompletableFuture<Void> handleReconfigureLeaveRequest(final Set<String> newMembers) {
    final CompletableFuture<Void> reconfigureComplete = new CompletableFuture<>();

    broker
        .reconfigureUpdateAll(newMembers)
        .thenApply(r -> reconfigureComplete.complete(r))
        .exceptionally(e -> reconfigureComplete.completeExceptionally(e));
    return reconfigureComplete;
  }

  private CompletableFuture<Void> handleLeaveRequest(final List<Integer> partitions) {
    final CompletableFuture<Void> leaveComplete = new CompletableFuture<>();

    CompletableFuture.allOf(
            partitions.stream().map(broker::leavePartition).toArray(CompletableFuture[]::new))
        .thenApply(r -> leaveComplete.complete(r))
        .exceptionally(e -> leaveComplete.completeExceptionally(e));
    return leaveComplete;
  }

  private CompletableFuture<Void> handleJoinRequest(final List<Integer> partitions) {
    final CompletableFuture<Void> joinComplete = new CompletableFuture<>();

    CompletableFuture.allOf(
            partitions.stream().map(broker::joinPartition).toArray(CompletableFuture[]::new))
        .thenApply(r -> joinComplete.complete(r))
        .exceptionally(e -> joinComplete.completeExceptionally(e));
    return joinComplete;
  }
}
