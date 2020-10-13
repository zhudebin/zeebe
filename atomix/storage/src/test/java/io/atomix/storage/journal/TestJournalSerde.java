package io.atomix.storage.journal;

import io.atomix.storage.protocol.EntryDecoder;
import io.atomix.storage.protocol.EntryEncoder;
import io.atomix.storage.protocol.MessageHeaderDecoder;
import io.atomix.storage.protocol.MessageHeaderEncoder;
import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class TestJournalSerde implements JournalSerde {

  private final ExpandableDirectByteBuffer writeBuffer = new ExpandableDirectByteBuffer();
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final EntryEncoder rfEncoder = new EntryEncoder();
  private final EntryDecoder rfDecoder = new EntryDecoder();

  public int serializeRaftLogEntry(final RaftLogEntry entry) {
    return serializeRaftLogEntry(writeBuffer, 0, entry);
  }

  @Override
  public int serializeRaftLogEntry(
      final MutableDirectBuffer buffer, final int offset, final RaftLogEntry entry) {
    headerEncoder
        .wrap(buffer, offset)
        .blockLength(rfEncoder.sbeBlockLength())
        .templateId(rfEncoder.sbeTemplateId())
        .schemaId(rfEncoder.sbeSchemaId())
        .version(rfEncoder.sbeSchemaVersion());

    rfEncoder.wrap(buffer, offset + headerEncoder.encodedLength());
    rfEncoder
        .term(entry.term())
        .timestamp(entry.timestamp())
        .entryType(entry.type())
        .putEntry(entry.entry(), 0, entry.entry().capacity());

    return headerEncoder.encodedLength() + rfEncoder.encodedLength();
  }

  @Override
  public RaftLogEntry deserializeRaftLogEntry(final DirectBuffer buffer, final int offset) {
    headerDecoder.wrap(buffer, offset);
    rfDecoder.wrap(
        buffer,
        offset + headerDecoder.encodedLength(),
        headerDecoder.blockLength(),
        headerDecoder.version());

    final UnsafeBuffer entryBuffer =
        new UnsafeBuffer(ByteBuffer.allocateDirect(rfDecoder.entryLength()));
    rfDecoder.getEntry(entryBuffer, 0, rfDecoder.entryLength());

    return new RaftLogEntry(
        rfDecoder.term(), rfDecoder.timestamp(), rfDecoder.entryType(), entryBuffer);
  }

  public RaftLogEntry deserializeRaftLogEntry() {
    return deserializeRaftLogEntry(0);
  }

  public RaftLogEntry deserializeRaftLogEntry(final int offset) {
    return deserializeRaftLogEntry(writeBuffer, offset);
  }
}
