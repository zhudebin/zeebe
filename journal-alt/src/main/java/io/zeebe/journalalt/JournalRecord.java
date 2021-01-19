package io.zeebe.journalalt;

import org.agrona.DirectBuffer;

public interface JournalRecord {

  long index();

  long getASQN();

  DirectBuffer getData();
}
