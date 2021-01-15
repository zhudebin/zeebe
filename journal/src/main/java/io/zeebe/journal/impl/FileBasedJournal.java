package io.zeebe.journal.impl;

import io.zeebe.journal.Journal;
import io.zeebe.journal.JournalReader;
import io.zeebe.journal.JournalRecord;
import org.agrona.DirectBuffer;

public class FileBasedJournal implements Journal {

  @Override
  public JournalRecord append(final DirectBuffer data) {
    return null;
  }

  @Override
  public void append(final JournalRecord record) throws Exception {

  }

  @Override
  public boolean deleteAfter(final long indexExclusive) {
    return false;
  }

  @Override
  public boolean deleteUntil(final long indexExclusive) {
    return false;
  }

  @Override
  public long getLastIndex() {
    return 0;
  }

  @Override
  public long getFirstIndex() {
    return 0;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public void flush() {

  }

  @Override
  public JournalReader openReader() {
    return null;
  }
}
