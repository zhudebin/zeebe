package io.atomix.raft.storage.log.entry;

import io.zeebe.journal.JournalRecord;
import org.agrona.DirectBuffer;

public interface RaftEntry extends JournalRecord {

  long term();

  boolean isApplicationEntry();

  ApplicationEntryImpl asApplicationEntry();

  boolean isConfigurationEntry();

  ConfigurationEntry asConfigurationEntry();

  boolean isInitialEntry();

  InitializeEntry asInitialEntry();

  DirectBuffer entry();

  int size();
}
