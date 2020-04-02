/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor.workflow.perf;

import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileChannelTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testFileChannel() throws Exception {
    // given

    final var dir = temporaryFolder.newFolder();
    // JournalSegment#openChannel
    final var fileChannel =
        FileChannel.open(
            dir.toPath(),
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE);

    //    this.memory = ByteBuffer.allocate((maxEntrySize + Integer.BYTES + Integer.BYTES) * 2);

  }

  //  @Override
  //  public void reset() {
  //    try {
  //      channel.position(JournalSegmentDescriptor.BYTES);
  //    } catch (final IOException e) {
  //      throw new StorageException(e);
  //    }
  //    memory.clear().limit(0);
  //    currentEntry = null;
  //    nextEntry = null;
  //    //    readNext();
  //  }

  //
  //        if (memory.remaining() < maxEntrySize) {
  //    final long position = channel.position() + memory.position();
  //    channel.position(position);
  //    memory.clear();
  //    channel.read(memory);
  //    channel.position(position);
  //    memory.flip();
  //  }

}
