package io.atomix.raft.storage.log.entry;

import org.agrona.MutableDirectBuffer;

public interface RaftEntry {

  void write(MutableDirectBuffer buffer);

  int getLength();

  default boolean isApplicationEntry() {
    return false;
  }

  default long getAsqn() {
    return -1;
  }
}
