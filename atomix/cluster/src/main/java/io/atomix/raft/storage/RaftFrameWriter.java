package io.atomix.raft.storage;

import io.atomix.raft.storage.impl.MessageHeaderEncoder;
import io.atomix.raft.storage.impl.RaftFrameEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class RaftFrameWriter {

  protected final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final RaftFrameEncoder encoder = new RaftFrameEncoder();
  private final long term;
  private final DirectBuffer data;

  public RaftFrameWriter(final long term, final DirectBuffer data) {
    this.term = term;
    this.data = data;
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
        + data.capacity();
  }

  public void write(final MutableDirectBuffer buffer, final int offset) {
    headerEncoder
        .wrap(buffer, offset)
        .blockLength(encoder.sbeBlockLength())
        .templateId(encoder.sbeTemplateId())
        .schemaId(encoder.sbeSchemaId())
        .version(encoder.sbeSchemaVersion());

    encoder.wrap(buffer, offset + headerEncoder.encodedLength());

    encoder.term(term).putData(data, 0, data.capacity());
  }
}
