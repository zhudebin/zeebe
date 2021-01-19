package io.zeebe.journal;

import java.util.Iterator;

public interface JournalReader extends Iterator<JournalRecord> {

  // if seek(index) return true
  //        next().index() == index
  // else hasNext() should return false.
  boolean seek(long index);

  long currentIndex();

  long nextIndex();
}
