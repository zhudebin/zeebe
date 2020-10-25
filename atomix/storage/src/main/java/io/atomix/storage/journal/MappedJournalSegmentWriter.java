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

import io.atomix.storage.StorageException;
import io.atomix.storage.journal.index.JournalIndex;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import org.agrona.IoUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Segment writer.
 *
 * <p>The format of an entry in the log is as follows:
 *
 * <ul>
 *   <li>64-bit index
 *   <li>8-bit boolean indicating whether a term change is contained in the entry
 *   <li>64-bit optional term
 *   <li>32-bit signed entry length, including the entry type ID
 *   <li>8-bit signed entry type ID
 *   <li>n-bit entry bytes
 * </ul>
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class MappedJournalSegmentWriter implements JournalWriter {

  private static final int ENTRY_PREAMBLE_SIZE = Integer.BYTES + Integer.BYTES;
  private final MappedByteBuffer buffer;
  private final JournalSegment segment;
  private final int maxEntrySize;
  private final JournalIndex journalIndex;
  private final JournalSerde serde;
  private final long firstIndex;
  private final Checksum checksum = new CRC32();
  private Indexed<Entry> lastEntry;
  private boolean isOpen = true;
  private final MutableDirectBuffer writeBuffer;

  MappedJournalSegmentWriter(
      final JournalSegmentFile file,
      final JournalSegment segment,
      final int maxEntrySize,
      final JournalIndex journalIndex,
      final JournalSerde serde) {
    buffer = mapFile(file, segment);
    this.segment = segment;
    this.maxEntrySize = maxEntrySize;
    this.journalIndex = journalIndex;
    this.serde = serde;
    firstIndex = segment.index();
    writeBuffer = new UnsafeBuffer(buffer);
    reset(0);
  }

  private static MappedByteBuffer mapFile(
      final JournalSegmentFile file, final JournalSegment segment) {
    // map existing file, because file is already created by SegmentedJournal
    return IoUtil.mapExistingFile(
        file.file(), file.name(), 0, segment.descriptor().maxSegmentSize());
  }

  @Override
  public long getLastIndex() {
    return lastEntry != null ? lastEntry.index() : segment.index() - 1;
  }

  @Override
  public Indexed<Entry> getLastEntry() {
    return lastEntry;
  }

  @Override
  public long getNextIndex() {
    if (lastEntry != null) {
      return lastEntry.index() + 1;
    } else {
      return firstIndex;
    }
  }

  @Override
  public <E extends Entry> Indexed<E> append(final E entry) {
    // Store the entry index.
    final long index = getNextIndex();
    final int offset = buffer.position();
    final int checksumOffset = offset + Integer.BYTES;
    final int entryOffset = checksumOffset + Integer.BYTES;

    final int entryLength = serde.computeEntryLength(entry);
    if ((entryLength + ENTRY_PREAMBLE_SIZE) > buffer.remaining()) {
      throw new BufferOverflowException();
    }

    // Serialize the entry.
    if (entryOffset + entryLength > buffer.limit()) {
      throw new BufferOverflowException();
    }

    // If the entry length exceeds the maximum entry size then throw an exception.
    if (entryLength > maxEntrySize) {
      throw new StorageException.TooLarge(
          "Entry size " + entryLength + " exceeds maximum allowed bytes (" + maxEntrySize + ")");
    }

    // write length so we can read back the correct entry
    writeBuffer.putInt(offset, entryLength, ByteOrder.LITTLE_ENDIAN);

    try {
      final int serializedLength = serde.serializeEntry(writeBuffer, entryOffset, entry);
      assert entryLength == serializedLength
          : "Expected computed length "
              + entryLength
              + " to be equal to serialized length "
              + serializedLength;
    } catch (final IndexOutOfBoundsException e) {
      throw new BufferOverflowException();
    }

    // Compute the checksum for the entry.
    checksum.reset();
    checksum.update(
        buffer.asReadOnlyBuffer().position(entryOffset).limit(entryOffset + entryLength));
    final int entryChecksum = (int) (checksum.getValue() & 0xFFFFFFFFL);
    writeBuffer.putInt(checksumOffset, entryChecksum, ByteOrder.LITTLE_ENDIAN);

    // Update the last entry with the correct index/term/length.
    final Indexed<E> indexedEntry = new Indexed<>(index, entry, entryLength);
    lastEntry = (Indexed<Entry>) indexedEntry;

    // update position in the buffer for the next entry
    buffer.position(entryOffset + entryLength);

    // index entry to make it efficiently searchable
    journalIndex.index(lastEntry, offset);
    return indexedEntry;
  }

  @Override
  public <E extends Entry> void append(final Indexed<E> entry) {
    final long nextIndex = getNextIndex();

    // If the entry's index is greater than the next index in the segment, skip some entries.
    if (entry.index() > nextIndex) {
      throw new IndexOutOfBoundsException("Entry index is not sequential");
    }

    // If the entry's index is less than the next index, truncate the segment.
    if (entry.index() < nextIndex) {
      truncate(entry.index() - 1);
    }
    append(entry.entry());
  }

  @Override
  public void commit(final long index) {}

  @Override
  public void reset(final long index) {
    long nextIndex = firstIndex;

    // Clear the buffer indexes.
    buffer.position(JournalSegmentDescriptor.BYTES);
    buffer.mark();

    try {
      int entryLength = writeBuffer.getInt(buffer.position(), ByteOrder.LITTLE_ENDIAN);

      // If the length is non-zero, read the entry.
      while (0 < entryLength && entryLength <= maxEntrySize && (index == 0 || nextIndex <= index)) {
        final int offset = buffer.position();
        final int checksumOffset = offset + Integer.BYTES;
        final int entryOffset = checksumOffset + Integer.BYTES;

        // Read the checksum of the entry.
        final int entryChecksum =
            (int) (writeBuffer.getInt(checksumOffset, ByteOrder.LITTLE_ENDIAN) & 0xFFFFFFFFL);

        // Compute the checksum for the entry bytes.
        checksum.reset();
        checksum.update(
            buffer.asReadOnlyBuffer().position(entryOffset).limit(entryOffset + entryLength));
        final int computedChecksum = (int) (checksum.getValue() & 0xFFFFFFFFL);

        // If the stored checksum equals the computed checksum, return the entry.
        if (entryChecksum == computedChecksum) {
          final Entry entry = serde.deserializeEntry(writeBuffer, entryOffset);
          lastEntry = new Indexed<>(nextIndex, entry, entryLength);
          journalIndex.index(lastEntry, offset);
          nextIndex++;
        } else {
          break;
        }

        // Update the current position for indexing.
        buffer.position(entryOffset + entryLength);
        buffer.mark();
        if (buffer.remaining() < Integer.BYTES) {
          break;
        }

        entryLength = writeBuffer.getInt(buffer.position(), ByteOrder.LITTLE_ENDIAN);
      }

      // Reset the buffer to the previous mark.
      buffer.reset();
    } catch (final BufferUnderflowException | IndexOutOfBoundsException e) {
      buffer.reset();
    }
  }

  @Override
  public void truncate(final long index) {
    // If the index is greater than or equal to the last index, skip the truncate.
    if (index >= getLastIndex()) {
      return;
    }

    // Reset the last entry.
    lastEntry = null;

    // Truncate the index.
    journalIndex.truncate(index);

    if (index < segment.index()) {
      buffer.position(JournalSegmentDescriptor.BYTES);
      buffer.putInt(0);
      buffer.putInt(0);
      buffer.position(JournalSegmentDescriptor.BYTES);
    } else {
      // Reset the writer to the given index.
      reset(index);

      // Zero entries after the given index.
      final int position = buffer.position();
      buffer.putInt(0);
      buffer.putInt(0);
      buffer.position(position);
    }
  }

  @Override
  public void flush() {
    buffer.force();
  }

  @Override
  public void close() {
    if (isOpen) {
      isOpen = false;
      flush();
      IoUtil.unmap(buffer);
    }
  }

  /**
   * Returns the size of the underlying buffer.
   *
   * @return The size of the underlying buffer.
   */
  public long size() {
    return buffer.position() + JournalSegmentDescriptor.BYTES;
  }

  /**
   * Returns a boolean indicating whether the segment is empty.
   *
   * @return Indicates whether the segment is empty.
   */
  public boolean isEmpty() {
    return lastEntry == null;
  }
}
