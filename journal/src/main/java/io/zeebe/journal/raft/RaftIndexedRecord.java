package io.zeebe.journal.raft;

public interface RaftIndexedRecord {
  long getIndex();

  long checksum();

  int getSize();

  RaftEntry getEntry();
}
