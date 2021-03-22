/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.zeebe.test.util.bpmn.random.blocks;

import io.zeebe.model.bpmn.builder.AbstractFlowNodeBuilder;
import io.zeebe.model.bpmn.builder.ParallelGatewayBuilder;
import io.zeebe.test.util.bpmn.random.AbstractExecutionStep;
import io.zeebe.test.util.bpmn.random.BlockBuilder;
import io.zeebe.test.util.bpmn.random.BlockBuilderFactory;
import io.zeebe.test.util.bpmn.random.ConstructionContext;
import io.zeebe.test.util.bpmn.random.ExecutionPathSegment;
import io.zeebe.test.util.bpmn.random.IDGenerator;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generates a block with a forking parallel gateway, a random number of branches, and a joining
 * parallel gateway. Each branch has a nested sequence of blocks.
 */
public class ParallelGatewayBlockBuilder implements BlockBuilder {

  private final List<BlockBuilder> blockBuilders = new ArrayList<>();
  private final List<String> branchIds = new ArrayList<>();
  private final String forkGatewayId;
  private final String joinGatewayId;

  public ParallelGatewayBlockBuilder(final ConstructionContext context) {
    final Random random = context.getRandom();
    final IDGenerator idGenerator = context.getIdGenerator();
    final int maxBranches = context.getMaxBranches();

    forkGatewayId = "fork_" + idGenerator.nextId();
    joinGatewayId = "join_" + idGenerator.nextId();

    final BlockSequenceBuilder.BlockSequenceBuilderFactory blockSequenceBuilderFactory =
        context.getBlockSequenceBuilderFactory();

    final int branches = Math.max(2, random.nextInt(maxBranches));

    for (int i = 0; i < branches; i++) {
      branchIds.add(idGenerator.nextId());
      blockBuilders.add(
          blockSequenceBuilderFactory.createBlockSequenceBuilder(context.withIncrementedDepth()));
    }
  }

  @Override
  public AbstractFlowNodeBuilder<?, ?> buildFlowNodes(
      final AbstractFlowNodeBuilder<?, ?> nodeBuilder) {
    final ParallelGatewayBuilder forkGateway = nodeBuilder.parallelGateway(forkGatewayId);

    AbstractFlowNodeBuilder<?, ?> workInProgress =
        blockBuilders.get(0).buildFlowNodes(forkGateway).parallelGateway(joinGatewayId);

    for (int i = 1; i < blockBuilders.size(); i++) {
      final String edgeId = branchIds.get(i);
      final BlockBuilder blockBuilder = blockBuilders.get(i);

      final AbstractFlowNodeBuilder<?, ?> outgoingEdge =
          workInProgress.moveToNode(forkGatewayId).sequenceFlowId(edgeId);

      workInProgress = blockBuilder.buildFlowNodes(outgoingEdge).connectTo(joinGatewayId);
    }

    return workInProgress;
  }

  @Override
  public ExecutionPathSegment findRandomExecutionPath(final Random random) {
    final ExecutionPathSegment result = new ExecutionPathSegment();

    result.append(new StepEnterParallelGateway(forkGatewayId));

    final List<List<AbstractExecutionStep>> stepsOfParallelPaths = new ArrayList<>();

    blockBuilders.forEach(
        blockBuilder ->
            stepsOfParallelPaths.add(
                new ArrayList<>(blockBuilder.findRandomExecutionPath(random).getSteps())));

    final List<AbstractExecutionStep> shuffledSteps =
        shuffleStepsFromDifferentLists(random, stepsOfParallelPaths);

    shuffledSteps.forEach(result::append);

    result.append(new StepLeaveParallelGateway(joinGatewayId));

    return result;
  }

  // shuffles the lists together by iteratively taking the first item from one of the lists
  private List<AbstractExecutionStep> shuffleStepsFromDifferentLists(
      final Random random, final List<List<AbstractExecutionStep>> sources) {
    final List<AbstractExecutionStep> result = new ArrayList<>();

    purgeEmptyLists(sources);

    while (!sources.isEmpty()) {
      final List<AbstractExecutionStep> source = sources.get(random.nextInt(sources.size()));

      final AbstractExecutionStep step = source.remove(0);
      result.add(step);
      purgeEmptyLists(sources);
    }

    return result;
  }

  private void purgeEmptyLists(final List<List<AbstractExecutionStep>> sources) {
    sources.removeIf(List::isEmpty);
  }

  public static final class StepEnterParallelGateway extends AbstractExecutionStep {

    private final String forkingGatewayId;

    public StepEnterParallelGateway(final String forkingGatewayId) {
      this.forkingGatewayId = forkingGatewayId;
    }

    @Override
    public boolean isAutomatic() {
      return true;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final StepEnterParallelGateway that = (StepEnterParallelGateway) o;

      if (forkingGatewayId != null
          ? !forkingGatewayId.equals(that.forkingGatewayId)
          : that.forkingGatewayId != null) {
        return false;
      }
      return variables.equals(that.variables);
    }

    @Override
    public int hashCode() {
      int result = forkingGatewayId != null ? forkingGatewayId.hashCode() : 0;
      result = 31 * result + variables.hashCode();
      return result;
    }
  }

  public static final class StepLeaveParallelGateway extends AbstractExecutionStep {

    private final String joiningGatewayId;

    public StepLeaveParallelGateway(final String joiningGatewayId) {
      this.joiningGatewayId = joiningGatewayId;
    }

    @Override
    public boolean isAutomatic() {
      return true;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final StepLeaveParallelGateway that = (StepLeaveParallelGateway) o;

      if (joiningGatewayId != null
          ? !joiningGatewayId.equals(that.joiningGatewayId)
          : that.joiningGatewayId != null) {
        return false;
      }
      return variables.equals(that.variables);
    }

    @Override
    public int hashCode() {
      int result = joiningGatewayId != null ? joiningGatewayId.hashCode() : 0;
      result = 31 * result + variables.hashCode();
      return result;
    }
  }

  public static class Factory implements BlockBuilderFactory {

    @Override
    public BlockBuilder createBlockBuilder(final ConstructionContext context) {
      return new ParallelGatewayBlockBuilder(context);
    }

    @Override
    public boolean isAddingDepth() {
      return true;
    }
  }
}
