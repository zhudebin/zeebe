package io.zeebe.journal;

import org.agrona.DirectBuffer;

public interface JournalRecord {

  long index();

  DirectBuffer getData();

  // more accessors depending on record format

}
