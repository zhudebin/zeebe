package io.zeebe.journal.raft;

// Wraps ZeebeEntry, InitializeEntry, ConfigurationEntry etc.
public interface RaftEntry<T> {

  long term();

  T entry();

  // Retrun class now because the current impl expects it, but it could be an int that encodes the type
  Class type();
}
