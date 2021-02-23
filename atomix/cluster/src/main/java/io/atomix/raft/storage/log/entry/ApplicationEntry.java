package io.atomix.raft.storage.log.entry;

import org.agrona.DirectBuffer;

public interface ApplicationEntry {
  long index();

  long term();

  long lowestAsqn();

  long highestAsqn();

  DirectBuffer data();
}
