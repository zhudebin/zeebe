/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.logstreams.storage.atomix;

import io.atomix.raft.partition.RaftPartition;
import io.zeebe.logstreams.storage.LogStorage;
import io.zeebe.logstreams.storage.LogStorageReader;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

/**
 * Implementation of {@link LogStorage} for the Atomix {@link io.atomix.raft.storage.log.RaftLog}.
 *
 * <p>Note that this class cannot be made final because we currently spy on it in our tests. This
 * should be changed when the log storage implementation is taken out of this module, at which point
 * it can be made final.
 */
public class AtomixLogStorage implements LogStorage {
  private final AtomixReaderFactory readerFactory;
  private final AtomixAppenderSupplier appenderSupplier;

  public AtomixLogStorage(
      final AtomixReaderFactory readerFactory, final AtomixAppenderSupplier appenderSupplier) {
    this.readerFactory = readerFactory;
    this.appenderSupplier = appenderSupplier;
  }

  public static AtomixLogStorage ofPartition(final RaftPartition partition) {
    final var server = new AtomixRaftServer(partition.getServer());
    return new AtomixLogStorage(server, server);
  }

  @Override
  public LogStorageReader newReader() {
    return new AtomixLogStorageReader(readerFactory.create());
  }

  @Override
  public void append(
      final long lowestPosition,
      final long highestPosition,
      final ByteBuffer buffer,
      final AppendListener listener) {
    final var optionalAppender = appenderSupplier.getAppender();

    if (optionalAppender.isPresent()) {
      final var appender = optionalAppender.get();
      final var adapter = new AtomixAppendListenerAdapter(listener);
      appender.appendEntry(lowestPosition, highestPosition, buffer, adapter);
    } else {
      listener.onWriteError(
          new NoSuchElementException(
              "Expected an appender, but none found, most likely we are not the leader"));
    }
  }
}
