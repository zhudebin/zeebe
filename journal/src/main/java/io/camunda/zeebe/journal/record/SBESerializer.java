/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.journal.record;

import io.camunda.zeebe.journal.CorruptedJournalException;
import io.camunda.zeebe.journal.file.MessageHeaderDecoder;
import io.camunda.zeebe.journal.file.MessageHeaderEncoder;
import io.camunda.zeebe.journal.file.RecordDataDecoder;
import io.camunda.zeebe.journal.file.RecordDataEncoder;
import io.camunda.zeebe.journal.file.RecordMetadataDecoder;
import io.camunda.zeebe.journal.file.RecordMetadataEncoder;
import io.camunda.zeebe.util.Either;
import java.nio.BufferOverflowException;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/** The serializer that writes and reads a journal record according to the SBE schema defined. */
public final class SBESerializer implements JournalRecordSerializer {
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final RecordMetadataEncoder metadataEncoder = new RecordMetadataEncoder();
  private final RecordDataEncoder recordEncoder = new RecordDataEncoder();

  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final RecordMetadataDecoder metadataDecoder = new RecordMetadataDecoder();
  private final RecordDataDecoder recordDecoder = new RecordDataDecoder();

  @Override
  public Either<BufferOverflowException, Integer> writeData(
      final RecordData record, final MutableDirectBuffer buffer, final int offset) {
    if (offset + getSerializedLength(record) > buffer.capacity()) {
      return Either.left(new BufferOverflowException());
    }

    headerEncoder
        .wrap(buffer, offset)
        .blockLength(recordEncoder.sbeBlockLength())
        .templateId(recordEncoder.sbeTemplateId())
        .schemaId(recordEncoder.sbeSchemaId())
        .version(recordEncoder.sbeSchemaVersion());

    recordEncoder.wrap(buffer, offset + headerEncoder.encodedLength());

    recordEncoder
        .index(record.index())
        .asqn(record.asqn())
        .putData(record.data(), 0, record.data().capacity());
    final var writtenBytes = headerEncoder.encodedLength() + recordEncoder.encodedLength();
    return Either.right(writtenBytes);
  }

  @Override
  public int writeMetadata(
      final RecordMetadata metadata, final MutableDirectBuffer buffer, final int offset) {

    headerEncoder
        .wrap(buffer, offset)
        .blockLength(metadataEncoder.sbeBlockLength())
        .templateId(metadataEncoder.sbeTemplateId())
        .schemaId(metadataEncoder.sbeSchemaId())
        .version(metadataEncoder.sbeSchemaVersion());

    metadataEncoder.wrap(buffer, offset + headerEncoder.encodedLength());

    metadataEncoder.checksum(metadata.checksum()).length(metadata.length());

    return headerEncoder.encodedLength() + metadataEncoder.encodedLength();
  }

  @Override
  public int getMetadataLength() {
    return headerEncoder.encodedLength() + metadataEncoder.sbeBlockLength();
  }

  @Override
  public RecordMetadata readMetadata(final DirectBuffer buffer, final int offset) {
    if (!hasMetadata(buffer, offset)) {
      throw new CorruptedJournalException("Cannot read metadata. Header does not match.");
    }
    metadataDecoder.wrap(
        buffer,
        offset + headerDecoder.encodedLength(),
        headerDecoder.blockLength(),
        headerDecoder.version());

    return new RecordMetadata(metadataDecoder.checksum(), metadataDecoder.length());
  }

  @Override
  public RecordData readData(final DirectBuffer buffer, final int offset) {
    headerDecoder.wrap(buffer, offset);
    if (headerDecoder.schemaId() != recordDecoder.sbeSchemaId()
        || headerDecoder.templateId() != recordDecoder.sbeTemplateId()) {
      throw new CorruptedJournalException("Cannot read record. Header does not match.");
    }
    recordDecoder.wrap(
        buffer,
        offset + headerDecoder.encodedLength(),
        headerDecoder.blockLength(),
        headerDecoder.version());

    final DirectBuffer data = new UnsafeBuffer();
    recordDecoder.wrapData(data);
    return new RecordData(recordDecoder.index(), recordDecoder.asqn(), data);
  }

  @Override
  public int getMetadataLength(final DirectBuffer buffer, final int offset) {
    headerDecoder.wrap(buffer, offset);
    return headerDecoder.encodedLength() + headerDecoder.blockLength();
  }

  private boolean hasMetadata(final DirectBuffer buffer, final int offset) {
    headerDecoder.wrap(buffer, offset);
    return (headerDecoder.schemaId() == metadataDecoder.sbeSchemaId()
        && headerDecoder.templateId() == metadataDecoder.sbeTemplateId());
  }

  private int getSerializedLength(final RecordData record) {
    return headerEncoder.encodedLength()
        + recordEncoder.sbeBlockLength()
        + RecordDataEncoder.dataHeaderLength()
        + record.data().capacity();
  }
}
