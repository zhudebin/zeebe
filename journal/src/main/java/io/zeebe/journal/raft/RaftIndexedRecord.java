package io.zeebe.journal.raft;

public interface RaftIndexedRecord {
  long index();

  long checksum();

  int size();

  RaftEntry entry();
}
