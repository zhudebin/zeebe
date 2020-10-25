/*
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.storage.journal;

import io.atomix.storage.protocol.ConfigurationEntryDecoder;
import io.atomix.storage.protocol.InitialEntryDecoder;
import io.atomix.storage.protocol.MessageHeaderDecoder;
import io.atomix.storage.protocol.MessageHeaderEncoder;
import io.atomix.storage.protocol.ZeebeEntryDecoder;
import io.atomix.storage.protocol.ZeebeEntryEncoder;
import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.sbe.MessageEncoderFlyweight;

public class TestJournalSerde implements JournalSerde {

  private final ExpandableDirectByteBuffer writeBuffer = new ExpandableDirectByteBuffer();
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final ZeebeEntryDecoder zeebeEntryDecoder = new ZeebeEntryDecoder();
  private final ZeebeEntryEncoder zeebeEntryEncoder = new ZeebeEntryEncoder();

  @Override
  public int computeEntryLength(final Entry entry) {
    switch (entry.type()) {
      case ZEEBE:
        return computeZeebeEntryLength((ZeebeEntry) entry);
      default:
        throw new IllegalArgumentException("Unexpected entry type " + entry.type());
    }
  }

  @Override
  public int serializeEntry(final MutableDirectBuffer buffer, final int offset, final Entry entry) {
    int length = headerEncoder.encodedLength();
    final int entryOffset = length + offset;
    final MessageEncoderFlyweight bodyEncoder;

    headerEncoder.wrap(buffer, offset);
    switch (entry.type()) {
      case ZEEBE:
        bodyEncoder = zeebeEntryEncoder;
        length += serializeZeebeEntry(buffer, entryOffset, (ZeebeEntry) entry);
        break;
      default:
        throw new IllegalArgumentException("Unexpected entry type " + entry.type());
    }

    headerEncoder
        .schemaId(headerEncoder.sbeSchemaId())
        .version(headerEncoder.sbeSchemaVersion())
        .templateId(bodyEncoder.sbeTemplateId())
        .blockLength(bodyEncoder.sbeBlockLength());
    return length;
  }

  @Override
  public Entry deserializeEntry(final DirectBuffer buffer, final int offset) {
    headerDecoder.wrap(buffer, offset);
    if (headerDecoder.schemaId() != MessageHeaderDecoder.SCHEMA_ID) {
      throw new IllegalArgumentException(
          "Expected schema ID to be "
              + MessageHeaderDecoder.SCHEMA_ID
              + " but got "
              + headerDecoder.schemaId());
    }

    final int entryOffset = headerDecoder.encodedLength() + offset;
    switch (headerDecoder.templateId()) {
      case ZeebeEntryDecoder.TEMPLATE_ID:
        return deserializeZeebeEntry(buffer, entryOffset);
      default:
        throw new IllegalArgumentException(
            "Expected a template ID of "
                + ZeebeEntryDecoder.TEMPLATE_ID
                + ", "
                + ConfigurationEntryDecoder.TEMPLATE_ID
                + ", or "
                + InitialEntryDecoder.TEMPLATE_ID
                + ", but got "
                + headerDecoder.templateId());
    }
  }

  int computeZeebeEntryLength(final ZeebeEntry entry) {
    return headerEncoder.encodedLength()
        + zeebeEntryEncoder.sbeBlockLength()
        + ZeebeEntryEncoder.dataHeaderLength()
        + entry.data().capacity();
  }

  int serializeZeebeEntry(
      final MutableDirectBuffer buffer, final int offset, final ZeebeEntry entry) {
    final DirectBuffer dataBuffer = entry.data();
    zeebeEntryEncoder
        .wrap(buffer, offset)
        .term(entry.term())
        .timestamp(entry.timestamp())
        .lowestPosition(entry.lowestPosition())
        .highestPosition(entry.highestPosition())
        .putData(dataBuffer, 0, dataBuffer.capacity());

    return zeebeEntryEncoder.sbeBlockLength()
        + ZeebeEntryEncoder.dataHeaderLength()
        + dataBuffer.capacity();
  }

  ZeebeEntry deserializeZeebeEntry(final DirectBuffer buffer, final int offset) {
    zeebeEntryDecoder.wrap(buffer, offset, headerDecoder.blockLength(), headerDecoder.version());

    final UnsafeBuffer entryBuffer =
        new UnsafeBuffer(ByteBuffer.allocateDirect(zeebeEntryDecoder.dataLength()));
    zeebeEntryDecoder.getData(entryBuffer, 0, entryBuffer.capacity());

    return new ZeebeEntry(
        zeebeEntryDecoder.term(),
        zeebeEntryDecoder.timestamp(),
        zeebeEntryDecoder.lowestPosition(),
        zeebeEntryDecoder.highestPosition(),
        entryBuffer);
  }
}
