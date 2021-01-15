package io.zeebe.journal.raft;

import java.util.ArrayList;
import java.util.List;

public class RaftRoles {

  private RaftJournal raftLog;
  // Just for illustration

  void leaderAppend() {
    final RaftEntry<Void> initialEntry = null;
    raftLog.append(initialEntry);

    final RaftEntry<Long> zeebeEntry = null;
    raftLog.append(zeebeEntry);
  }

  void leaderAppender() {
    // replicate to member

    // buildAppendRequest
    final var reader = raftLog.openReader();
    reader.seek(nextMemberIndex);

    final List<RaftIndexedRecord> entriesToReplicate = new ArrayList<>();
    int totalSize = 0;
    while(totalSize <= maxSize) {
       final var nextEntry = reader.next();
       totalSize += nextEntry.getSize();
       entriesToReplicate.add(nextEntry);
    }
    final AppendRequest request = new AppendRequest(0, leader, prevIndex, prevTerm, entriesToReplicate, commitIndex);
  }

  void onAppend(final List<RaftIndexedRecord> records) throws Exception {
    // Verify term, previous term etc.
    final var reader = raftLog.openReader();
    reader.seek(raftLog.getLastIndex());
    final var previousEntry = reader.next();
    // verify previousEntry.getEntry().term() == appendRequest.previousTerm
    final RaftEntry entry = previousEntry.getEntry();
    entry.getTerm(); // verify
    // verify previousEntry.index() == appendRequest.previousIndex;
    previousEntry.getIndex(); // Verify
    for (final RaftIndexedRecord record : records) {
      raftLog.append(record);
    }
  }

  class AppendRequest {
    AppendRequest(
        final long term,
        final String leader,
        final long prevLogIndex,
        final long prevLogTerm,
        final List<RaftIndexedRecord> entries,
        final long commitIndex) {
    }
  }
}
