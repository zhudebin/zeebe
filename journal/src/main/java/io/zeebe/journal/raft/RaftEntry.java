package io.zeebe.journal.raft;

public interface RaftEntry<T> {

  long getTerm();

  int entryType();

  T entry();
}
