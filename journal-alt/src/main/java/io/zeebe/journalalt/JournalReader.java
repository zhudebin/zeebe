package io.zeebe.journalalt;

import java.util.Iterator;

public interface JournalReader extends Iterator<JournalRecord> {

  // seek to the exact index
  boolean seek(long index);

  // approximate seek since asqn can have gaps
  boolean seekAsqn(long asqn);

}
