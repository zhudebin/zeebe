package io.zeebe.journal.raft;

import io.zeebe.journal.Journal;
import io.zeebe.journal.JournalRecord;

public class RaftJournal {

  private volatile long commitPosition;
  private final Journal journal;

  public RaftJournal(final Journal journal) {
    this.journal = journal;
  }

  public long getCommitPosition() {
    return commitPosition;
  }

  public RaftJournal setCommitPosition(final long commitPosition) {
    this.commitPosition = commitPosition;
    return this;
  }

  public <T> void append(final RaftEntry<T> entry) {}

  public void append(final RaftIndexedRecord entry) throws Exception {
    final JournalRecord recordToAppend =
        null; // new JournalRecordImpl(entry.getLongIndex(), entry.checksum(), entry.getEntry());
    journal.append(recordToAppend);
  }

  public boolean deleteAfter(final long indexExclusive) {
    return journal.deleteAfter(indexExclusive);
  }

  public boolean deleteUntil(final long indexExclusive) {
    return journal.deleteUntil(indexExclusive);
  }

  public void clearAndReset(final long nextIndex) {
    journal.clearAndReset(nextIndex);
  }

  public long getLastIndex() {
    return journal.getLastIndex();
  }

  public RaftIndexedRecord getLastEntry() {
    return null; // TODO
  }

  public long getFirstIndex() {
    return journal.getFirstIndex();
  }

  public boolean isEmpty() {
    return journal.isEmpty();
  }

  public void flush() {
    journal.flush();
  }

  public RaftLogReader openReader() {
    // TODO
    return null;
  }

  public void close() {
    journal.close();
  }
}
