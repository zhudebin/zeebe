package io.atomix.raft.storage;

import io.atomix.raft.storage.impl.MessageHeaderEncoder;
import io.atomix.raft.storage.impl.RaftFrameEncoder;
import io.atomix.raft.storage.log.entry.RaftEntry;
import java.nio.ByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class RaftFrameWriter implements RaftEntry {

  protected final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final RaftFrameEncoder encoder = new RaftFrameEncoder();
  private final long term;
  private final RaftEntry entry;

  // TODO: Pass RaftLogEntryWriter here and call write to avoid copying twice
  public RaftFrameWriter(final long term, final RaftEntry entry) {
    this.term = term;
    this.entry = entry;
  }

  /**
   * Returns the length required to write this record to a buffer
   *
   * @return the length
   */
  public int getLength() {
    return headerEncoder.encodedLength()
        + encoder.sbeBlockLength()
        + RaftFrameEncoder.dataHeaderLength()
        + entry.getLength();
  }

  @Override
  public boolean isApplicationEntry() {
    return entry.isApplicationEntry();
  }

  @Override
  public long getAsqn() {
    return entry.getAsqn();
  }

  @Override
  public void write(final MutableDirectBuffer buffer) {
    final MutableDirectBuffer entryBuffer =
        new UnsafeBuffer(ByteBuffer.allocate(entry.getLength()));
    entry.write(entryBuffer);

    headerEncoder
        .wrap(buffer, 0)
        .blockLength(encoder.sbeBlockLength())
        .templateId(encoder.sbeTemplateId())
        .schemaId(encoder.sbeSchemaId())
        .version(encoder.sbeSchemaVersion());

    encoder.wrap(buffer, headerEncoder.encodedLength());
    encoder.term(term).putData(entryBuffer, 0, entry.getLength());
  }
}
