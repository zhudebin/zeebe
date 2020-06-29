/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.test.integration.containers;

import io.zeebe.containers.ZeebeBrokerContainer;
import io.zeebe.test.integration.Cluster;
import io.zeebe.test.integration.DelegatingCluster;
import io.zeebe.test.integration.containers.configurators.exporters.DebugHttpExporterConfigurator;
import java.net.URI;
import org.junit.rules.ExternalResource;
import org.testcontainers.containers.Network;

public class DockerDebugHttpCluster extends ExternalResource implements DelegatingCluster {

  private static final int DEFAULT_EXPORTER_PORT = 8000;
  private final DockerCluster clusterRule;

  private int port = DEFAULT_EXPORTER_PORT;

  public DockerDebugHttpCluster(final DockerCluster clusterRule) {
    this.clusterRule = clusterRule;
  }

  @Override
  public void before() throws Throwable {
    resolveNetwork();
    final var configurator = new DebugHttpExporterConfigurator(port);

    clusterRule.withBrokerConfigurator(configurator);
    clusterRule.before();
  }

  @Override
  public void after() {
    clusterRule.after();
  }

  public DockerDebugHttpCluster withPort(final int port) {
    this.port = port;
    return this;
  }

  @Override
  public Cluster getClusterDelegate() {
    return clusterRule;
  }

  public URI getExporterURI(final ZeebeBrokerContainer broker) {
    final var address =
        String.format("%s:%d", broker.getContainerIpAddress(), broker.getMappedPort(port));
    return URI.create(address);
  }

  private void resolveNetwork() {
    final var network = clusterRule.getNetwork();
    if (network == null) {
      clusterRule.withNetwork(Network.newNetwork());
    }
  }
}
