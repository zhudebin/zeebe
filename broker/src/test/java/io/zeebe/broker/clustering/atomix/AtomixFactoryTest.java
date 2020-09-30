/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.clustering.atomix;

import static org.assertj.core.api.Assertions.assertThat;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.core.Atomix;
import io.atomix.raft.partition.RaftPartitionGroup;
import io.atomix.raft.partition.RaftPartitionGroupConfig;
import io.atomix.storage.StorageLevel;
import io.atomix.utils.net.Address;
import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.broker.system.configuration.SocketBindingCfg;
import io.zeebe.broker.system.configuration.SocketBindingCfg.CommandApiCfg;
import io.zeebe.snapshots.broker.impl.FileBasedSnapshotStoreFactory;
import io.zeebe.test.util.socket.SocketUtil;
import io.zeebe.util.Environment;
import java.util.Collection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public final class AtomixFactoryTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private Environment environment;

  @Before
  public void setUp() {
    environment = new Environment();
  }

  @Test
  public void shouldUseMappedStorageLevel() {
    // given
    final var brokerConfig = newConfig();
    brokerConfig.getData().setUseMmap(true);

    // when
    final var atomix =
        AtomixFactory.createAtomix(brokerConfig, new FileBasedSnapshotStoreFactory());

    // then
    final var config = getPartitionGroupConfig(atomix);
    assertThat(config.getStorageConfig().getLevel()).isEqualTo(StorageLevel.MAPPED);
  }

  @Test
  public void shouldUseDiskStorageLevel() {
    // given
    final var brokerConfig = newConfig();
    brokerConfig.getData().setUseMmap(false);

    // when
    final var atomix =
        AtomixFactory.createAtomix(brokerConfig, new FileBasedSnapshotStoreFactory());

    // then
    final var config = getPartitionGroupConfig(atomix);
    assertThat(config.getStorageConfig().getLevel()).isEqualTo(StorageLevel.DISK);
  }

  @Test
  public void shouldDisableExplicitFlush() {
    // given
    final var brokerConfig = newConfig();
    brokerConfig.getExperimental().setDisableExplicitRaftFlush(true);

    // when
    final var atomix =
        AtomixFactory.createAtomix(brokerConfig, new FileBasedSnapshotStoreFactory());

    // then
    final var config = getPartitionGroupConfig(atomix);
    assertThat(config.getStorageConfig().shouldFlushExplicitly()).isFalse();
  }

  @Test
  public void shouldEnableExplicitFlush() {
    // given
    final var brokerConfig = newConfig();
    brokerConfig.getExperimental().setDisableExplicitRaftFlush(false);

    // when
    final var atomix =
        AtomixFactory.createAtomix(brokerConfig, new FileBasedSnapshotStoreFactory());

    // then
    final var config = getPartitionGroupConfig(atomix);
    assertThat(config.getStorageConfig().shouldFlushExplicitly()).isTrue();
  }

  @Test
  public void shouldBindInternalApiHostToMessagingInterface() {
    // given
    final var brokerConfig = newConfig();
    final SocketBindingCfg internalApi = brokerConfig.getNetwork().getInternalApi();
    internalApi.setHost("127.0.0.1");
    internalApi.setPort(SocketUtil.getNextAddress().getPort());

    // when
    final var atomix =
        AtomixFactory.createAtomix(brokerConfig, new FileBasedSnapshotStoreFactory());

    // then
    final Collection<Address> addresses = atomix.getMessagingService().addresses();
    assertThat(addresses)
        .containsExactly(Address.from(internalApi.getHost(), internalApi.getPort()));
  }

  @Test
  public void shouldAdvertiseInternalApiAddress() {
    // given
    final BrokerCfg brokerConfig = newConfig();
    final SocketBindingCfg internalApi = brokerConfig.getNetwork().getInternalApi();
    internalApi.setAdvertisedHost("foo");
    internalApi.setAdvertisedPort(8080);

    // when
    final var atomix =
        AtomixFactory.createAtomix(brokerConfig, new FileBasedSnapshotStoreFactory());

    // then
    assertThat(atomix.getMessagingService().advertisedAddress())
        .isEqualTo(Address.from("foo", 8080));
  }

  @Test
  public void shouldCreateNewMessagingService() {
    // given
    final BrokerCfg brokerConfig = newConfig();
    final CommandApiCfg commandApi = brokerConfig.getNetwork().getCommandApi();
    commandApi.setAdvertisedHost("foo");
    commandApi.setAdvertisedPort(8080);

    // when
    final ManagedMessagingService messagingService =
        AtomixFactory.createMessagingService(brokerConfig.getCluster(), commandApi);

    // then
    assertThat(messagingService.advertisedAddress()).isEqualTo(Address.from("foo", 8080));
    assertThat(messagingService.addresses())
        .containsExactly(Address.from(commandApi.getHost(), commandApi.getPort()));
  }

  private RaftPartitionGroup getPartitionGroup(final Atomix atomix) {
    return (RaftPartitionGroup)
        atomix.getPartitionService().getPartitionGroup(AtomixFactory.GROUP_NAME);
  }

  private RaftPartitionGroupConfig getPartitionGroupConfig(final Atomix atomix) {
    return (RaftPartitionGroupConfig) getPartitionGroup(atomix).config();
  }

  private BrokerCfg newConfig() {
    final var config = new BrokerCfg();
    config.init(temporaryFolder.getRoot().getAbsolutePath(), environment);

    return config;
  }
}
