package io.zeebe.journalalt;

import org.agrona.DirectBuffer;

public interface Journal {

  // asqn = Application Sequence Number
  // asqn can be a "IGNORE value" for control records like initialize entry or configuration
  // entry. If not IGNORE value, it should be always increasing. asqn can have gaps.
  JournalRecord append(long asqn, DirectBuffer data);

  JournalRecord append(JournalRecord record);
}
