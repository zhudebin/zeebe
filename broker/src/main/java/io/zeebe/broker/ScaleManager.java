package io.zeebe.broker;

import io.atomix.core.Atomix;
import io.zeebe.util.sched.Actor;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ScaleManager extends Actor {

  private final Atomix atomix;
  private final Broker broker;

  public ScaleManager(final Atomix atomix, final Broker broker) {
    this.atomix = atomix;
    this.broker = broker;
  }

  @Override
  protected void onActorStarted() {
    atomix.getCommunicationService().subscribe("join-partitions", this::handleJoinRequest);
    atomix.getCommunicationService().subscribe("leave-partitions", this::handleLeaveRequest);
  }

  private CompletableFuture<Void> handleLeaveRequest(final List<Integer> partitions) {
    final CompletableFuture<Void> leaveComplete = new CompletableFuture<>();
    actor.run(
        () ->
            CompletableFuture.allOf(
                    partitions.stream()
                        .map(broker::leavePartition)
                        .toArray(CompletableFuture[]::new))
                .thenApply(r -> leaveComplete.complete(r))
                .exceptionally(e -> leaveComplete.completeExceptionally(e)));
    return leaveComplete;
  }

  private CompletableFuture<Void> handleJoinRequest(final List<Integer> partitions) {
    final CompletableFuture<Void> joinComplete = new CompletableFuture<>();
    actor.run(
        () ->
            CompletableFuture.allOf(
                    partitions.stream()
                        .map(broker::joinPartition)
                        .toArray(CompletableFuture[]::new))
                .thenApply(r -> joinComplete.complete(r))
                .exceptionally(e -> joinComplete.completeExceptionally(e)));
    return joinComplete;
  }
}
