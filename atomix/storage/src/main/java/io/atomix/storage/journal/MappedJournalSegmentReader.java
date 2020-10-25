/*
 * Copyright 2017-present Open Networking Foundation
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.storage.journal;

import io.atomix.storage.journal.index.JournalIndex;
import io.atomix.storage.journal.index.Position;
import java.nio.BufferUnderflowException;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.NoSuchElementException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Log segment reader.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class MappedJournalSegmentReader implements JournalReader {
  private final MappedByteBuffer buffer;
  private final int maxEntrySize;
  private final JournalIndex journalIndex;
  private final JournalSerde serde;
  private final JournalSegment segment;
  private final DirectBuffer readBuffer;
  private final Checksum checksum = new CRC32();
  private Indexed<Entry> currentEntry;
  private Indexed<Entry> nextEntry;

  MappedJournalSegmentReader(
      final JournalSegmentFile file,
      final JournalSegment segment,
      final int maxEntrySize,
      final JournalIndex journalIndex,
      final JournalSerde serde) {
    buffer =
        IoUtil.mapExistingFile(
            file.file(), MapMode.READ_ONLY, file.name(), 0, segment.descriptor().maxSegmentSize());
    this.maxEntrySize = maxEntrySize;
    this.journalIndex = journalIndex;
    this.serde = serde;
    this.segment = segment;
    readBuffer = new UnsafeBuffer(buffer);
    reset();
  }

  @Override
  public boolean isEmpty() {
    return buffer.limit() == 0;
  }

  @Override
  public long getFirstIndex() {
    return segment.index();
  }

  @Override
  public long getLastIndex() {
    return segment.lastIndex();
  }

  @Override
  public long getCurrentIndex() {
    return currentEntry != null ? currentEntry.index() : 0;
  }

  @Override
  public Indexed<Entry> getCurrentEntry() {
    return currentEntry;
  }

  @Override
  public long getNextIndex() {
    return currentEntry != null ? currentEntry.index() + 1 : getFirstIndex();
  }

  @Override
  public boolean hasNext() {
    // If the next entry is null, check whether a next entry exists.
    if (nextEntry == null) {
      readNext();
    }
    return nextEntry != null;
  }

  @Override
  public Indexed<Entry> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    // Set the current entry to the next entry.
    currentEntry = nextEntry;

    // Reset the next entry to null.
    nextEntry = null;

    // Read the next entry in the segment.
    readNext();

    // Return the current entry.
    return currentEntry;
  }

  @Override
  public void reset() {
    buffer.position(JournalSegmentDescriptor.BYTES);
    currentEntry = null;
    nextEntry = null;
    readNext();
  }

  @Override
  public void reset(final long index) {
    final long firstIndex = segment.index();
    final long lastIndex = segment.lastIndex();

    reset();

    final Position position = journalIndex.lookup(index - 1);
    if (position != null && position.index() >= firstIndex && position.index() <= lastIndex) {
      currentEntry = new Indexed<>(position.index() - 1, null, 0);
      buffer.position(position.position());

      nextEntry = null;
      readNext();
    }

    while (getNextIndex() < index && hasNext()) {
      next();
    }
  }

  @Override
  public void close() {
    IoUtil.unmap(buffer);
    segment.onReaderClosed(this);
  }

  /** Reads the next entry in the segment. */
  private void readNext() {
    // Compute the index of the next entry in the segment.
    final long index = getNextIndex();
    final int offset = buffer.position();
    final int checksumOffset = offset + Integer.BYTES;
    final int entryOffset = checksumOffset + Integer.BYTES;

    try {
      if (entryOffset > buffer.capacity()) {
        nextEntry = null;
        return;
      }

      // Read the length of the entry.
      final int entryLength = readBuffer.getInt(offset, ByteOrder.LITTLE_ENDIAN);

      // If the buffer length is zero then return.
      if (entryLength <= 0 || entryLength > maxEntrySize) {
        nextEntry = null;
        return;
      }

      // Read the checksum of the entry.
      final int entryChecksum = (int) (readBuffer.getInt(checksumOffset) & 0xFFFFFFFFL);

      // Compute the checksum for the entry bytes.
      checksum.reset();
      checksum.update(
          buffer.asReadOnlyBuffer().position(entryOffset).limit(entryOffset + entryLength));
      final int computedChecksum = (int) (checksum.getValue() & 0xFFFFFFFFL);

      // If the stored checksum equals the computed checksum, return the entry.
      if (entryChecksum == computedChecksum) {
        final Entry entry = serde.deserializeEntry(readBuffer, entryOffset);
        buffer.position(entryOffset + entryLength);
        nextEntry = new Indexed<>(index, entry, entryLength);
      }
    } catch (final BufferUnderflowException | IndexOutOfBoundsException e) {
      nextEntry = null;
    }
  }
}
