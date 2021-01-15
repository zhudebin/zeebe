package io.atomix.raft.storage.log;

import io.atomix.storage.journal.JournalReader.Mode;
import io.zeebe.journal.JournalReader;
import io.zeebe.journal.JournalRecord;

public class RaftJournalReader implements JournalReader {

  private final RaftJournal journal;
  private final JournalReader reader;
  private final Mode isolationMode;

  public RaftJournalReader(
      final RaftJournal journal, final JournalReader reader, final Mode isolationMode) {

    this.journal = journal;
    this.reader = reader;
    this.isolationMode = isolationMode;
  }

  @Override
  public boolean seek(final long index) {
    // Check mode
    return reader.seek(index);
  }

  @Override
  public boolean hasNext() {
    return reader.hasNext();
  }

  @Override
  public JournalRecord next() {
    return reader.next();
  }
}
