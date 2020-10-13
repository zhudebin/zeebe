package io.atomix.storage.journal;

import io.atomix.storage.protocol.EntryEncoder;
import io.atomix.storage.protocol.MessageHeaderEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public interface JournalSerde {
  default int computeSerializedLength(final RaftLogEntry entry) {
    return MessageHeaderEncoder.ENCODED_LENGTH
        + EntryEncoder.BLOCK_LENGTH
        + EntryEncoder.entryHeaderLength()
        + entry.entry().capacity();
  }

  int serializeRaftLogEntry(
      final MutableDirectBuffer buffer, final int offset, final RaftLogEntry entry);

  RaftLogEntry deserializeRaftLogEntry(final DirectBuffer buffer, final int offset);
}
