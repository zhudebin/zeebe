package io.atomix.raft.storage.log.entry;

import io.atomix.raft.storage.impl.MessageHeaderEncoder;
import io.atomix.raft.storage.impl.ZeebeEntryEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class ApplicationEntryWriter {

  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final ZeebeEntryEncoder encoder = new ZeebeEntryEncoder();
  private final long lowestPosition;
  private final long highestPosition;
  private final DirectBuffer data;

  public ApplicationEntryWriter(
      final long lowestPosition, final long highestPosition, final DirectBuffer data) {
    this.lowestPosition = lowestPosition;
    this.highestPosition = highestPosition;
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
        + ZeebeEntryEncoder.dataHeaderLength()
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

    encoder
        .lowestPosition(lowestPosition)
        .highestPosition(highestPosition)
        .putData(data, 0, data.capacity());
  }
}
