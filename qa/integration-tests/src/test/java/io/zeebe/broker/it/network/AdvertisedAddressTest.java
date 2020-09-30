/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.it.network;

import io.zeebe.client.ZeebeClient;
import io.zeebe.client.ZeebeClientBuilder;
import io.zeebe.client.api.response.Topology;
import io.zeebe.containers.ZeebeContainer;
import io.zeebe.containers.ZeebePort;
import io.zeebe.containers.ZeebeTopologyWaitStrategy;
import io.zeebe.test.util.asserts.TopologyAssert;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;

/**
 * Almost works - what's missing is that for some reason the members get weird UUIDs instead of the
 * correct node IDs
 */
public class AdvertisedAddressTest {
  private static final String TOXIPROXY_NETWORK_ALIAS = "toxiproxy";
  private static final String TOXIPROXY_IMAGE = "shopify/toxiproxy:2.1.0";

  @Rule public final Network network = Network.newNetwork();

  @Rule
  public final ToxiproxyContainer toxiproxy =
      new ToxiproxyContainer(TOXIPROXY_IMAGE)
          .withNetwork(network)
          .withNetworkAliases(TOXIPROXY_NETWORK_ALIAS);

  private List<ZeebeContainer> containers;
  private List<String> initialContactPoints;

  @Before
  public void setup() {
    initialContactPoints = new ArrayList<>();
    containers =
        Arrays.asList(
            new ZeebeContainer("camunda/zeebe:current-test"),
            new ZeebeContainer("camunda/zeebe:current-test"),
            new ZeebeContainer("camunda/zeebe:current-test"));

    configureBrokerContainer(0, containers);
    configureBrokerContainer(1, containers);
    configureBrokerContainer(2, containers);
  }

  @After
  public void tearDown() {
    containers.parallelStream().forEach(GenericContainer::stop);
  }

  @Test
  public void shouldProxy() {
    // given
    containers.parallelStream().forEach(GenericContainer::start);

    // when
    final var availableContainer = containers.get(0);
    final ZeebeClientBuilder zeebeClientBuilder =
        ZeebeClient.newClientBuilder()
            .usePlaintext()
            .gatewayAddress(availableContainer.getExternalGatewayAddress());
    final Topology topology;
    try (final var client = zeebeClientBuilder.build()) {
      topology = client.newTopologyRequest().send().join(5, TimeUnit.SECONDS);
    }

    // then
    TopologyAssert.assertThat(topology).isComplete(3, 1);
  }

  private void configureBrokerContainer(final int index, final List<ZeebeContainer> brokers) {
    final int clusterSize = brokers.size();
    final var broker = brokers.get(index);
    final var hostName = "broker-" + index;
    final var commandApiProxy = toxiproxy.getProxy(hostName, ZeebePort.COMMAND.getPort());
    final var internalApiProxy = toxiproxy.getProxy(hostName, ZeebePort.INTERNAL.getPort());
    final var monitoringApiProxy = toxiproxy.getProxy(hostName, ZeebePort.MONITORING.getPort());

    initialContactPoints.add(
        internalApiProxy.getContainerIpAddress() + ":" + internalApiProxy.getProxyPort());

    broker
        .withNetwork(network)
        .withNetworkAliases(hostName)
        .withTopologyCheck(
            new ZeebeTopologyWaitStrategy()
                .forBrokersCount(3)
                .forPartitionsCount(1)
                .forReplicationFactor(3))
        .withEnv("ZEEBE_BROKER_NETWORK_MAXMESSAGESIZE", "128KB")
        .withEnv("ZEEBE_BROKER_CLUSTER_NODEID", String.valueOf(index))
        .withEnv("ZEEBE_BROKER_CLUSTER_CLUSTERSIZE", String.valueOf(clusterSize))
        .withEnv("ZEEBE_BROKER_CLUSTER_REPLICATIONFACTOR", String.valueOf(clusterSize))
        .withEnv(
            "ZEEBE_BROKER_CLUSTER_INITIALCONTACTPOINTS", String.join(",", initialContactPoints))
        .withEnv("ZEEBE_LOG_LEVEL", "INFO")
        .withEnv("ATOMIX_LOG_LEVEL", "INFO")
        .withEnv(
            "ZEEBE_BROKER_NETWORK_COMMANDAPI_ADVERTISEDHOST",
            commandApiProxy.getContainerIpAddress())
        .withEnv(
            "ZEEBE_BROKER_NETWORK_COMMANDAPI_ADVERTISEDPORT",
            String.valueOf(commandApiProxy.getProxyPort()))
        .withEnv(
            "ZEEBE_BROKER_NETWORK_INTERNALAPI_ADVERTISEDHOST",
            internalApiProxy.getContainerIpAddress())
        .withEnv(
            "ZEEBE_BROKER_NETWORK_INTERNALAPI_ADVERTISEDPORT",
            String.valueOf(internalApiProxy.getProxyPort()))
        .withEnv(
            "ZEEBE_BROKER_NETWORK_MONITORINGAPI_ADVERTISEDHOST",
            monitoringApiProxy.getContainerIpAddress())
        .withEnv(
            "ZEEBE_BROKER_NETWORK_MONITORINGAPI_ADVERTISEDPORT",
            String.valueOf(monitoringApiProxy.getProxyPort()));
  }
}
