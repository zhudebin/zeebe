package io.zeebe.journal;

import org.agrona.DirectBuffer;

public interface Journal {

  JournalRecord append(DirectBuffer data);

  // Exceptions thrown when ChecksumInvalid or UnexpectedIndex
  void append(JournalRecord record) throws Exception;

  // Guaranteed to delete all entries after indexExclusive
  boolean deleteAfter(long indexExclusive);

  // Not guaranteed to delete. But more like marking that it is safe to delete. The entries may be deleted immediately or later.
  boolean deleteUntil(long indexExclusive);

  boolean clearAndReset(long nextIndex);

  long getLastIndex();

  long getFirstIndex();

  boolean isEmpty();

  void flush();

  JournalReader openReader();

  void close();
}
