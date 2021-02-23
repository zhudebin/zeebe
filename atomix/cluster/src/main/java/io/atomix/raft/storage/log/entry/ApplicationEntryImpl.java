package io.atomix.raft.storage.log.entry;

import org.agrona.DirectBuffer;

public class ApplicationEntryImpl implements ApplicationEntry {

  private final RaftEntry raftEntry;
  private final ApplicationEntryReader reader;

  public ApplicationEntryImpl(final RaftEntry raftEntry) {
    this.raftEntry = raftEntry;
    this.reader = new ApplicationEntryReader(raftEntry.entry());
  }

  public long index() {
    return raftEntry.index();
  }

  public long term() {
    return raftEntry.term();
  }

  public long lowestAsqn() {
    return reader.lowestPosition();
  }

  public long highestAsqn() {
    return reader.highestPosition();
  }

  public DirectBuffer data() {
    return reader.data();
  }
}
