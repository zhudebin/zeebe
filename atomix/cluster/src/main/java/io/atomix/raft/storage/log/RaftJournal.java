package io.atomix.raft.storage.log;

import io.atomix.raft.storage.log.entry.RaftLogEntry;
import io.atomix.storage.journal.JournalReader.Mode;
import io.zeebe.journal.Journal;

public class RaftJournal {

  private volatile long commitIndex = 0;
  private final Journal journal;

  public RaftJournal(final Journal journal) {
    this.journal = journal;
  }

  public void append(final RaftLogEntry entry) {
    // journal.append(serialized entry)
  }

  public void commit(final long index) {
    commitIndex = Math.max(commitIndex, index);
  }

  public long getCommitIndex() {
    return commitIndex;
  }

  public RaftJournalReader openReader(final Mode isolationMode) {
    final var reader = journal.openReader();
    return new RaftJournalReader(this, reader, isolationMode);
  }
}
