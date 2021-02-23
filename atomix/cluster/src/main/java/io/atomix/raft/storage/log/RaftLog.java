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
package io.atomix.raft.storage.log;

import io.atomix.raft.storage.RaftFrameReader;
import io.atomix.raft.storage.RaftFrameWriter;
import io.atomix.raft.storage.log.RaftLogReader.Mode;
import io.atomix.raft.storage.log.entry.ApplicationEntryWriter;
import io.atomix.raft.storage.log.entry.ConfigurationEntry;
import io.atomix.raft.storage.log.entry.InitializeEntry;
import io.atomix.raft.storage.log.entry.RaftEntry;
import io.atomix.raft.storage.log.entry.RaftEntryImpl;
import io.atomix.raft.storage.log.entry.RaftLogEntry;
import io.atomix.raft.zeebe.ZeebeEntry;
import io.zeebe.journal.Journal;
import io.zeebe.journal.JournalRecord;
import io.zeebe.journal.file.SegmentedJournal;
import io.zeebe.journal.file.SegmentedJournalBuilder;
import java.io.Closeable;
import java.io.File;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/** Raft log. */
public class RaftLog implements Closeable {
  private final Journal journal;
  private final boolean flushExplicitly;

  private RaftEntry lastAppendedEntry;
  private volatile long commitIndex;
  private final MutableDirectBuffer buffer = new ExpandableArrayBuffer();

  protected RaftLog(final Journal journal, final boolean flushExplicitly) {
    this.journal = journal;
    this.flushExplicitly = flushExplicitly;
  }

  /**
   * Returns a new Raft log builder.
   *
   * @return A new Raft log builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  public RaftLogReader openReader(final long index) {
    return openReader(index, Mode.ALL);
  }

  public RaftLogReader openReader(final long index, final Mode mode) {
    final RaftLogReader reader = new RaftLogReader(this, journal.openReader(), mode);
    reader.reset(index);

    return reader;
  }

  public boolean isOpen() {
    return journal.isOpen();
  }

  /**
   * Compacts the journal up to the given index.
   *
   * <p>The semantics of compaction are not specified by this interface.
   *
   * @param index The index up to which to compact the journal.
   */
  public void compact(final long index) {
    journal.deleteUntil(index);
  }

  /**
   * Returns the Raft log commit index.
   *
   * @return The Raft log commit index.
   */
  public long getCommitIndex() {
    return commitIndex;
  }

  /**
   * Commits entries up to the given index.
   *
   * @param index The index up to which to commit entries.
   */
  public void setCommitIndex(final long index) {
    commitIndex = index;
  }

  public boolean shouldFlushExplicitly() {
    return flushExplicitly;
  }

  public long getFirstIndex() {
    return journal.getFirstIndex();
  }

  public long getLastIndex() {
    return journal.getLastIndex();
  }

  public RaftEntry getLastEntry() {
    if (lastAppendedEntry == null) {
      readLastEntry();
    }

    return lastAppendedEntry;
  }

  private void readLastEntry() {
    try (final var reader = openReader(journal.getLastIndex())) {
      if (reader.hasNext()) {
        lastAppendedEntry = reader.next();
      }
    }
  }

  public boolean isEmpty() {
    return journal.isEmpty();
  }

  public <T extends RaftLogEntry> RaftEntry append(final T entry) {
    final DirectBuffer wrappedEntryBuffer = new UnsafeBuffer();
    MutableDirectBuffer entryBuffer = new ExpandableArrayBuffer();

    if (entry instanceof ZeebeEntry) {
      final ZeebeEntry zbEntry = (ZeebeEntry) entry;
      final ApplicationEntryWriter appEntryWriter =
          new ApplicationEntryWriter(
              zbEntry.lowestPosition(),
              zbEntry.highestPosition(),
              new UnsafeBuffer(zbEntry.data()));
      appEntryWriter.write(entryBuffer, 0);
      wrappedEntryBuffer.wrap(entryBuffer, 0, appEntryWriter.getLength());
    } else if (entry instanceof ConfigurationEntry) {

    } else if (entry instanceof InitializeEntry) {

    }

    final RaftFrameWriter writer = new RaftFrameWriter(entry.term(), wrappedEntryBuffer);

    final DirectBuffer b = new UnsafeBuffer();
    writer.write(buffer, 0);
    b.wrap(buffer, 0, writer.getLength());

    final JournalRecord journalRecord;
    if (entry instanceof ZeebeEntry) {
      final ZeebeEntry asqnEntry = (ZeebeEntry) entry;
      journalRecord = journal.append(asqnEntry.lowestPosition(), b);
    } else {
      journalRecord = journal.append(b);
    }

    final RaftFrameReader reader = new RaftFrameReader(journalRecord.data());
    final RaftEntry writtenEntry = new RaftEntryImpl(reader, journalRecord);
    lastAppendedEntry = writtenEntry;

    return writtenEntry;
  }

  public void reset(final long index) {
    journal.reset(index);
    lastAppendedEntry = null;
  }

  public void truncate(final long index) {
    journal.deleteAfter(index);
    lastAppendedEntry = null;
  }

  public void flush() {
    if (flushExplicitly) {
      journal.flush();
    }
  }

  @Override
  public void close() {
    CloseHelper.close(journal);
  }

  @Override
  public String toString() {
    return "RaftLog{"
        + "journal="
        + journal
        + ", flushExplicitly="
        + flushExplicitly
        + ", lastWrittenEntry="
        + lastAppendedEntry
        + ", commitIndex="
        + commitIndex
        + '}';
  }

  public static class Builder implements io.atomix.utils.Builder<RaftLog> {

    private final SegmentedJournalBuilder journalBuilder = SegmentedJournal.builder();
    private boolean flushExplicitly = true;

    protected Builder() {}

    /**
     * Sets the storage name.
     *
     * @param name The storage name.
     * @return The storage builder.
     */
    public Builder withName(final String name) {
      journalBuilder.withName(name);
      return this;
    }

    /**
     * Sets the log directory, returning the builder for method chaining.
     *
     * <p>The log will write segment files into the provided directory.
     *
     * @param directory The log directory.
     * @return The storage builder.
     * @throws NullPointerException If the {@code directory} is {@code null}
     */
    public Builder withDirectory(final String directory) {
      journalBuilder.withDirectory(directory);
      return this;
    }

    /**
     * Sets the log directory, returning the builder for method chaining.
     *
     * <p>The log will write segment files into the provided directory.
     *
     * @param directory The log directory.
     * @return The storage builder.
     * @throws NullPointerException If the {@code directory} is {@code null}
     */
    public Builder withDirectory(final File directory) {
      journalBuilder.withDirectory(directory);
      return this;
    }

    /**
     * Sets the maximum segment size in bytes, returning the builder for method chaining.
     *
     * <p>The maximum segment size dictates when logs should roll over to new segments. As entries
     * are written to a segment of the log, once the size of the segment surpasses the configured
     * maximum segment size, the log will create a new segment and append new entries to that
     * segment.
     *
     * <p>By default, the maximum segment size is {@code 1024 * 1024 * 32}.
     *
     * @param maxSegmentSize The maximum segment size in bytes.
     * @return The storage builder.
     * @throws IllegalArgumentException If the {@code maxSegmentSize} is not positive
     */
    public Builder withMaxSegmentSize(final int maxSegmentSize) {
      journalBuilder.withMaxSegmentSize(maxSegmentSize);
      return this;
    }

    /**
     * Sets the maximum entry size in bytes, returning the builder for method chaining.
     *
     * @param maxEntrySize the maximum entry size in bytes
     * @return the storage builder
     * @throws IllegalArgumentException if the {@code maxEntrySize} is not positive
     */
    public Builder withMaxEntrySize(final int maxEntrySize) {
      journalBuilder.withMaxEntrySize(maxEntrySize);
      return this;
    }

    /**
     * Sets the minimum free disk space to leave when allocating a new segment
     *
     * @param freeDiskSpace free disk space in bytes
     * @return the storage builder
     * @throws IllegalArgumentException if the {@code freeDiskSpace} is not positive
     */
    public Builder withFreeDiskSpace(final long freeDiskSpace) {
      journalBuilder.withFreeDiskSpace(freeDiskSpace);
      return this;
    }

    /**
     * Sets whether or not to flush buffered I/O explicitly at various points, returning the builder
     * for chaining.
     *
     * <p>Enabling this ensures that entries are flushed on followers before acknowledging a write,
     * and are flushed on the leader before marking an entry as committed. This guarantees the
     * correctness of various Raft properties.
     *
     * @param flushExplicitly whether to flush explicitly or not
     * @return this builder for chaining
     */
    public Builder withFlushExplicitly(final boolean flushExplicitly) {
      this.flushExplicitly = flushExplicitly;
      return this;
    }

    public Builder withJournalIndexDensity(final int journalIndexDensity) {
      journalBuilder.withJournalIndexDensity(journalIndexDensity);
      return this;
    }

    @Override
    public RaftLog build() {
      final Journal journal = journalBuilder.build();
      return new RaftLog(journal, flushExplicitly);
    }
  }
}
