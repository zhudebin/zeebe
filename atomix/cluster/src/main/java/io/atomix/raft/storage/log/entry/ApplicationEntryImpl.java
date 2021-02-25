package io.atomix.raft.storage.log.entry;

import io.atomix.raft.storage.impl.MessageHeaderDecoder;
import io.atomix.raft.storage.impl.MessageHeaderEncoder;
import io.atomix.raft.storage.impl.ZeebeEntryDecoder;
import io.atomix.raft.storage.impl.ZeebeEntryEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class ApplicationEntryImpl implements ApplicationEntry {

  private long lowestAsqn;
  private long highestAsqn;
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final ZeebeEntryDecoder decoder = new ZeebeEntryDecoder();
  private final DirectBuffer data = new UnsafeBuffer();

  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final ZeebeEntryEncoder encoder = new ZeebeEntryEncoder();

  public ApplicationEntryImpl(final DirectBuffer buffer) {
    wrap(buffer);
    decoder.wrapData(data);
    this.lowestAsqn = decoder.lowestPosition();
    this.highestAsqn = decoder.highestPosition();
  }

  public ApplicationEntryImpl(
      final long lowestAsqn, final long highestAsqn, final DirectBuffer data) {
    this.lowestAsqn = lowestAsqn;
    this.highestAsqn = highestAsqn;
    this.data.wrap(data);
  }

  public long lowestAsqn() {
    return lowestAsqn;
  }

  public long highestAsqn() {
    return highestAsqn;
  }

  public DirectBuffer data() {
    return data;
  }

  @Override
  public void write(final MutableDirectBuffer buffer) {
    headerEncoder
        .wrap(buffer, 0)
        .blockLength(encoder.sbeBlockLength())
        .templateId(encoder.sbeTemplateId())
        .schemaId(encoder.sbeSchemaId())
        .version(encoder.sbeSchemaVersion());

    encoder.wrap(buffer, headerEncoder.encodedLength());

    encoder
        .lowestPosition(lowestAsqn)
        .highestPosition(highestAsqn)
        .putData(data, 0, data.capacity());
  }

  public long lowestPosition() {
    return decoder.lowestPosition();
  }

  public long highestPosition() {
    return decoder.highestPosition();
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
