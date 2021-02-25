package io.atomix.raft.storage.log.entry;

import org.agrona.DirectBuffer;

public interface ApplicationEntry extends RaftEntry {

  long lowestAsqn();

  long highestAsqn();

  DirectBuffer data();
}
