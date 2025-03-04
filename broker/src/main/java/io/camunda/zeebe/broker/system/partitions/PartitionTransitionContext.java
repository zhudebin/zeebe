/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.system.partitions;

import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.raft.RaftServer.Role;
import io.camunda.zeebe.broker.PartitionListener;
import io.camunda.zeebe.broker.exporter.repo.ExporterDescriptor;
import io.camunda.zeebe.broker.exporter.stream.ExporterDirector;
import io.camunda.zeebe.broker.logstreams.AtomixLogStorage;
import io.camunda.zeebe.broker.system.configuration.BrokerCfg;
import io.camunda.zeebe.broker.system.monitoring.DiskSpaceUsageMonitor;
import io.camunda.zeebe.broker.system.partitions.impl.AsyncSnapshotDirector;
import io.camunda.zeebe.broker.transport.partitionapi.InterPartitionCommandReceiverActor;
import io.camunda.zeebe.db.ZeebeDb;
import io.camunda.zeebe.engine.api.TypedRecord;
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecordProcessorFactory;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.CommandResponseWriter;
import io.camunda.zeebe.engine.state.QueryService;
import io.camunda.zeebe.logstreams.log.LogStream;
import io.camunda.zeebe.scheduler.ActorSchedulingService;
import io.camunda.zeebe.scheduler.ConcurrencyControl;
import io.camunda.zeebe.streamprocessor.StreamProcessor;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

public interface PartitionTransitionContext extends PartitionContext {

  int getNodeId();

  LogStream getLogStream();

  void setLogStream(LogStream logStream);

  AsyncSnapshotDirector getSnapshotDirector();

  void setSnapshotDirector(AsyncSnapshotDirector snapshotDirector);

  StateController getStateController();

  List<PartitionListener> getPartitionListeners();

  PartitionContext getPartitionContext();

  void setStreamProcessor(StreamProcessor streamProcessor);

  void setCurrentTerm(long term);

  void setCurrentRole(Role role);

  ActorSchedulingService getActorSchedulingService();

  ZeebeDb getZeebeDb();

  void setZeebeDb(ZeebeDb zeebeDb);

  CommandResponseWriter getCommandResponseWriter();

  Consumer<TypedRecord<?>> getOnProcessedListener();

  TypedRecordProcessorFactory getStreamProcessorFactory();

  ConcurrencyControl getConcurrencyControl();

  void setConcurrencyControl(ConcurrencyControl concurrencyControl);

  void setExporterDirector(ExporterDirector exporterDirector);

  PartitionMessagingService getMessagingService();

  ClusterCommunicationService getClusterCommunicationService();

  InterPartitionCommandReceiverActor getPartitionCommandReceiver();

  void setPartitionCommandReceiver(InterPartitionCommandReceiverActor receiver);

  boolean shouldExport();

  Collection<ExporterDescriptor> getExportedDescriptors();

  AtomixLogStorage getLogStorage();

  void setLogStorage(AtomixLogStorage logStorage);

  int getMaxFragmentSize();

  BrokerCfg getBrokerCfg();

  QueryService getQueryService();

  void setQueryService(QueryService queryService);

  DiskSpaceUsageMonitor getDiskSpaceUsageMonitor();
}
