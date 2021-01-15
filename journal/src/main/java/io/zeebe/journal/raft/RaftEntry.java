package io.zeebe.journal.raft;

public interface RaftEntry<T> {

  long term();

  T entry();

  Class type();
}
