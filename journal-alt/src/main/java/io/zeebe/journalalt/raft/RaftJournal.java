package io.zeebe.journalalt.raft;

import io.zeebe.journalalt.Journal;
import io.zeebe.journalalt.JournalRecord;
import org.agrona.DirectBuffer;

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

  public <T> void append(final RaftEntry<T> entry) {
    // DirectBuffer data = serialize(entry);
    // journal.append(entry.asqn(), data);
  }

  public void append(final RaftIndexedRecord entry) throws Exception {
    final DirectBuffer record = entry.data();
    final JournalRecord recordToAppend = null; // TODO
    // final JournalRecordImpl recordToAppend = new JournalRecordImpl();
    // recordToAppend.wrap(record);
    journal.append(recordToAppend);
  }
}
