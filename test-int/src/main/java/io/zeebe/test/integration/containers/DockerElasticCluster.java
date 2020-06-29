/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.test.integration.containers;

import io.zeebe.exporter.ElasticsearchExporterConfiguration;
import io.zeebe.test.integration.Cluster;
import io.zeebe.test.integration.DelegatingCluster;
import io.zeebe.test.integration.containers.configurators.exporters.ElasticsearchExporterConfigurator;
import java.util.Optional;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.rules.ExternalResource;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.lifecycle.Startable;

public class DockerElasticCluster extends ExternalResource implements DelegatingCluster {
  private static final String DEFAULT_EXPORTER_ID = "elastic";
  private static final String NETWORK_ALIAS = "elastic";
  private static final String DOCKER_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch";

  private final DockerCluster clusterRule;

  private ElasticsearchContainer elasticContainer;
  private ElasticsearchExporterConfiguration exporterConfig;

  public DockerElasticCluster(final DockerCluster clusterRule) {
    this.clusterRule = clusterRule;
  }

  @Override
  public void before() throws Throwable {
    resolveExporterConfig();
    resolveNetwork();

    final var configurator =
        new ElasticsearchExporterConfigurator(DEFAULT_EXPORTER_ID, exporterConfig);
    clusterRule.withBrokerConfigurator(configurator);
    elasticContainer = newDefaultElasticContainer();

    elasticContainer.start();
    clusterRule.before();
  }

  @Override
  public void after() {
    clusterRule.after();
    Optional.ofNullable(elasticContainer).ifPresent(Startable::stop);
  }

  public DockerElasticCluster withExporterConfig(
      final ElasticsearchExporterConfiguration exporterConfig) {
    this.exporterConfig = exporterConfig;
    return this;
  }

  public ElasticsearchContainer getElasticContainer() {
    return elasticContainer;
  }

  @Override
  public Cluster getClusterDelegate() {
    return clusterRule;
  }

  @SuppressWarnings("java:S2095")
  private ElasticsearchContainer newDefaultElasticContainer() {
    final var version = RestClient.class.getPackage().getImplementationVersion();
    final var logger = LoggerFactory.getLogger(DockerElasticCluster.class.getName() + ".elastic");
    return new ElasticsearchContainer(DOCKER_IMAGE + ":" + version)
        .withNetwork(clusterRule.getNetwork())
        .withNetworkAliases(NETWORK_ALIAS)
        .withLogConsumer(new Slf4jLogConsumer(logger, true))
        .withEnv("discovery.type", "single-node");
  }

  private RestClient newElasticClient() {
    final var host = HttpHost.create(elasticContainer.getHttpHostAddress());
    return RestClient.builder(host).build();
  }

  private void resolveExporterConfig() {
    final var config = Optional.ofNullable(exporterConfig).orElse(newDefaultExporterConfig());
    config.url = String.format("http://%s:9200", NETWORK_ALIAS);

    exporterConfig = config;
  }

  private void resolveNetwork() {
    final var network = clusterRule.getNetwork();
    if (network == null) {
      clusterRule.withNetwork(Network.newNetwork());
    }
  }

  private ElasticsearchExporterConfiguration newDefaultExporterConfig() {
    final var config = new ElasticsearchExporterConfiguration();

    // set a very small delay/size to achieve something closer to streaming - terrible in production
    // but for testing should be alright unless stress testing
    config.bulk.delay = 1;
    config.bulk.size = 1;

    // ensure we export everything out by default
    config.index.command = true;
    config.index.event = true;
    config.index.rejection = true;
    config.index.deployment = true;
    config.index.error = true;
    config.index.incident = true;
    config.index.job = true;
    config.index.jobBatch = true;
    config.index.message = true;
    config.index.messageSubscription = true;
    config.index.variable = true;
    config.index.variableDocument = true;
    config.index.workflowInstance = true;
    config.index.workflowInstanceCreation = true;
    config.index.workflowInstanceSubscription = true;

    return config;
  }
}
