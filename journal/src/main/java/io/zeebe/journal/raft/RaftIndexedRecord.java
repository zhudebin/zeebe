package io.zeebe.journal.raft;

import org.agrona.DirectBuffer;

public interface RaftIndexedRecord {

  long index();

  // Not necessary - but current AppendEntry request expects this. Keep this until refactoring.
  long checksum();

  RaftEntry entry();

  // JournalRecord
  DirectBuffer data();
}
