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

import io.atomix.raft.storage.log.entry.RaftLogEntry;
import io.zeebe.journal.JournalReader;
import io.zeebe.journal.JournalRecord;
import java.util.NoSuchElementException;

/** Raft log reader. */
public class RaftLogReader implements java.util.Iterator<IndexedRaftLogEntry>, AutoCloseable {
  private final RaftLog log;
  private final JournalReader journalReader;
  private final RaftLogReader.Mode mode;
  private final RaftEntrySerializer serializer = new RaftEntrySBESerializer();

  // NOTE: nextIndex is only used if the reader is in commit mode, hence why it's not subject to
  // inconsistencies when the log is truncated/compacted/etc.
  private long nextIndex;

  RaftLogReader(
      final RaftLog log, final JournalReader journalReader, final RaftLogReader.Mode mode) {
    this.log = log;
    this.journalReader = journalReader;
    this.mode = mode;

    nextIndex = log.getFirstIndex();
  }

  @Override
  public boolean hasNext() {
    return (mode == Mode.ALL || nextIndex <= log.getCommitIndex()) && journalReader.hasNext();
  }

  @Override
  public IndexedRaftLogEntry next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    final JournalRecord journalRecord = journalReader.next();
    final RaftLogEntry entry = serializer.readRaftLogEntry(journalRecord.data());

    nextIndex = journalRecord.index() + 1;
    return new IndexedRaftLogEntryImpl(entry.term(), entry.entry(), journalRecord);
  }

  public long reset() {
    nextIndex = journalReader.seekToFirst();
    return nextIndex;
  }

  public long reset(final long index) {
    if (nextIndex == index) {
      return nextIndex;
    }

    long boundedIndex = index;

    if (mode == Mode.COMMITS) {
      // allow seeking one past the commit index to simulate being at the end of the log
      final long upperBoundIndex = log.getCommitIndex() + 1;
      boundedIndex = Math.min(index, upperBoundIndex);
    }

    nextIndex = journalReader.seek(boundedIndex);
    return nextIndex;
  }

  public long seekToLast() {
    if (mode == Mode.ALL) {
      nextIndex = journalReader.seekToLast();
    } else {
      reset(log.getCommitIndex());
    }

    return nextIndex;
  }

  public long seekToAsqn(final long asqn) {
    nextIndex = journalReader.seekToAsqn(asqn);

    if (nextIndex > log.getCommitIndex() && !log.isEmpty()) {
      throw new UnsupportedOperationException("Cannot seek to an ASQN that is not yet committed");
    }

    return nextIndex;
  }

  @Override
  public void close() {
    journalReader.close();
  }

  /** Raft log reader mode. */
  public enum Mode {

    /** Reads all entries from the log. */
    ALL,

    /** Reads committed entries from the log. */
    COMMITS,
  }
}
