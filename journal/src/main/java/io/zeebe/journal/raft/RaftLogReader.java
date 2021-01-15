package io.zeebe.journal.raft;

import io.zeebe.journal.JournalReader;
import io.zeebe.journal.JournalRecord;

public class RaftLogReader {

  private final JournalReader reader;

  public RaftLogReader(final RaftJournal journal) {
    reader = journal.openReader();
  }

  public boolean seek(final long index) {
    return reader.seek(index);
  }

  public boolean hasNext() {
    return reader.hasNext();
  }

  public RaftIndexedRecord next() {
    final JournalRecord record = reader.next();
    // TODO: return new RaftIndexedEntryImpl(record.index(), record.getChecksum(), record.getData()) {
    return null;
  }
}
