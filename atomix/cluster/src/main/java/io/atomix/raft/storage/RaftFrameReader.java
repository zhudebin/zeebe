package io.atomix.raft.storage;

import io.atomix.raft.storage.impl.MessageHeaderDecoder;
import io.atomix.raft.storage.impl.RaftFrameDecoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class RaftFrameReader {
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final RaftFrameDecoder decoder = new RaftFrameDecoder();
  private final DirectBuffer data = new UnsafeBuffer();

  public RaftFrameReader(final DirectBuffer buffer) {
    wrap(buffer);
    decoder.wrapData(data);
  }

  public long term() {
    return decoder.term();
  }

  public DirectBuffer data() {
    return data;
  }

  public int getLength() {
    return headerDecoder.encodedLength()
        + headerDecoder.blockLength()
        + RaftFrameDecoder.dataHeaderLength()
        + data.capacity();
  }

  public void wrap(final DirectBuffer buffer) {
    wrap(buffer, 0, buffer.capacity());
  }

  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    if (!canRead(buffer, 0)) {
      throw new RuntimeException("Cannot read buffer"); // TODO
    }

    headerDecoder.wrap(buffer, offset);
    decoder.wrap(
        buffer,
        offset + headerDecoder.encodedLength(),
        headerDecoder.blockLength(),
        headerDecoder.version());
  }

  public boolean canRead(final DirectBuffer buffer, final int offset) {
    headerDecoder.wrap(buffer, offset);
    return (headerDecoder.schemaId() == decoder.sbeSchemaId()
        && headerDecoder.templateId() == decoder.sbeTemplateId());
  }
}
