package io.zeebe.journalalt.raft;

public interface RaftEntry<T> {

  long asqn(); // can be a NULL value for raft controll records like initialize entry or

  long term();

  T entry();

  Class type();

}
