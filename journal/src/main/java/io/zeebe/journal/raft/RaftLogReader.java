package io.zeebe.journal.raft;

import io.zeebe.journal.Journal;
import io.zeebe.journal.JournalReader;


// TODO: (Optional) Make this an interface??
// TODO: Two different types of reader - one used internally in raft, one used by Zeebe??
public class RaftLogReader {

  private final JournalReader reader;
  private RaftIndexedRecord currentEntry;
  private Mode readMode; // Committed or Uncommitted

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
    // currentEntry = new RaftIndexedRecordImpl(record)
    // return currentEntry;
    return null;
  }

  // Required for Raft at several places
  public RaftIndexedRecord getCurrentEntry() {
    return currentEntry;
  }
}
