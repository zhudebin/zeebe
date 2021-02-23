package io.atomix.raft.storage.log.entry;

import io.atomix.raft.storage.impl.MessageHeaderDecoder;
import io.atomix.raft.storage.impl.ZeebeEntryDecoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class ApplicationEntryReader {

  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final ZeebeEntryDecoder decoder = new ZeebeEntryDecoder();
  private final DirectBuffer data = new UnsafeBuffer();

  public ApplicationEntryReader(final DirectBuffer buffer) {
    wrap(buffer);
    decoder.wrapData(data);
  }

  public long lowestPosition() {
    return decoder.lowestPosition();
  }

  public long highestPosition() {
    return decoder.highestPosition();
  }

  public DirectBuffer data() {
    return data;
  }

  public int getLength() {
    return headerDecoder.encodedLength()
        + headerDecoder.blockLength()
        + ZeebeEntryDecoder.dataHeaderLength()
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
