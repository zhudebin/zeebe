/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.it.clustering;

import io.zeebe.broker.Broker;
import io.zeebe.broker.it.util.GrpcClientRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
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
  public void shouldBecomeFollowerAfterRestartLeaderChange() throws InterruptedException {
    // given
    final Broker broker = clusteringRule.createBroker(2);
    Thread.sleep(5000);
    broker.joinPartition(3).join();
    clusteringRule.getBroker(0).leavePartition(3).join();
    Thread.sleep(20000);
  }
}
