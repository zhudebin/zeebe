package io.zeebe.logstreams.util;

import io.atomix.raft.storage.log.entry.ApplicationEntry;
import org.agrona.DirectBuffer;

public class TestApplicationEntry implements ApplicationEntry {


  private final long lowestAsqn;
  private final long highestAsqn;
  private final DirectBuffer data;

  public TestApplicationEntry(

      final long lowestAsqn,
      final long highestAsqn,
      final DirectBuffer data) {

    this.lowestAsqn = lowestAsqn;
    this.highestAsqn = highestAsqn;
    this.data = data;
  }

  @Override
  public long lowestAsqn() {
    return lowestAsqn;
  }

  @Override
  public long highestAsqn() {
    return highestAsqn;
  }

  @Override
  public DirectBuffer data() {
    return data;
  }
}
