package io.zeebe.gateway;

import io.zeebe.gateway.impl.broker.cluster.BrokerTopologyManager;
import java.util.Set;

public class ScaleManager {

  final BrokerTopologyManager topologyManager;

  public ScaleManager(final BrokerTopologyManager topologyManager) {
    this.topologyManager = topologyManager;
  }

  public void handleNewMemberJoin(final Set<String> newMembers) {}
}
