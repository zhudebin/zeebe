/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.test.integration;

import io.zeebe.broker.system.configuration.ClusterCfg;
import io.zeebe.client.ZeebeClient;
import io.zeebe.containers.ZeebeBrokerContainer;
import io.zeebe.containers.ZeebeStandaloneGatewayContainer;
import java.util.Map;

public interface DelegatingCluster extends Cluster {
  Cluster getClusterDelegate();

  @Override
  default ZeebeClient getClient() {
    return getClusterDelegate().getClient();
  }

  @Override
  default ZeebeBrokerContainer getBroker(final int nodeId) {
    return getClusterDelegate().getBroker(nodeId);
  }

  @Override
  default Map<Integer, ZeebeBrokerContainer> getBrokers() {
    return getClusterDelegate().getBrokers();
  }

  @Override
  default ZeebeStandaloneGatewayContainer getGateway() {
    return getClusterDelegate().getGateway();
  }

  @Override
  default ClusterCfg getClusterConfig() {
    return getClusterDelegate().getClusterConfig();
  }

  @Override
  default ClusterInspector getInspector() {
    return getClusterDelegate().getInspector();
  }
}
