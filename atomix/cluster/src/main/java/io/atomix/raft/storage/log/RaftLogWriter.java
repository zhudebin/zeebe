package io.atomix.raft.storage.log;

import io.atomix.raft.storage.log.entry.RaftLogEntry;
import io.atomix.storage.journal.JournalWriter;

public interface RaftLogWriter extends JournalWriter<RaftLogEntry> {}
