package io.zeebe.journal.raft;

import io.zeebe.journal.Journal;
import io.zeebe.journal.JournalReader;

public class RaftLogReader {

  private final JournalReader reader;
  private RaftIndexedRecord currentEntry;

  public RaftLogReader(final RaftJournal raft, final Journal journal) {
    reader = journal.openReader();
  }

  public boolean seek(final long index) {
    return reader.seek(index);
  }

  public boolean hasNext() {
    return reader.hasNext();
  }

  public RaftIndexedRecord next() {
    final var record = reader.next();
    // TODO:
    //  currentEntry = new RaftIndexedRecordImpl(record)
    // return currentEntry;
    return null;
  }

  // Required for Raft to build AppendRequest
  public RaftIndexedRecord getCurrentEntry() {
    return currentEntry;
  }
}
