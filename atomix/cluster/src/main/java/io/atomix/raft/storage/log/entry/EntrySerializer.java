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
package io.atomix.raft.storage.log.entry;

import io.atomix.cluster.MemberId;
import io.atomix.raft.cluster.RaftMember;
import io.atomix.raft.cluster.RaftMember.Type;
import io.atomix.raft.cluster.impl.DefaultRaftMember;
import io.atomix.storage.journal.ConfigurationEntry;
import io.atomix.storage.journal.ConfigurationEntryMember;
import io.atomix.storage.journal.Entry;
import io.atomix.storage.journal.InitialEntry;
import io.atomix.storage.journal.JournalSerde;
import io.atomix.storage.journal.ZeebeEntry;
import io.atomix.storage.protocol.ConfigurationEntryDecoder;
import io.atomix.storage.protocol.ConfigurationEntryEncoder;
import io.atomix.storage.protocol.ConfigurationEntryEncoder.RaftMembersEncoder;
import io.atomix.storage.protocol.InitialEntryDecoder;
import io.atomix.storage.protocol.InitialEntryEncoder;
import io.atomix.storage.protocol.MessageHeaderDecoder;
import io.atomix.storage.protocol.MessageHeaderEncoder;
import io.atomix.storage.protocol.Role;
import io.atomix.storage.protocol.ZeebeEntryDecoder;
import io.atomix.storage.protocol.ZeebeEntryEncoder;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.sbe.MessageEncoderFlyweight;

public class EntrySerializer implements JournalSerde {

  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final ZeebeEntryDecoder zeebeEntryDecoder = new ZeebeEntryDecoder();
  private final ZeebeEntryEncoder zeebeEntryEncoder = new ZeebeEntryEncoder();
  private final ConfigurationEntryEncoder configurationEntryEncoder =
      new ConfigurationEntryEncoder();
  private final ConfigurationEntryDecoder configurationEntryDecoder =
      new ConfigurationEntryDecoder();
  private final InitialEntryEncoder initialEntryEncoder = new InitialEntryEncoder();
  private final InitialEntryDecoder initialEntryDecoder = new InitialEntryDecoder();

  @Override
  public int computeEntryLength(final Entry entry) {
    switch (entry.type()) {
      case ZEEBE:
        return computeZeebeEntryLength((ZeebeEntry) entry);
      case CONFIGURATION:
        return computeConfigurationEntryLength(
            (ConfigurationEntry<ConfigurationEntryMember>) entry);
      case INITIALIZE:
        return computeInitialEntryLength();
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
      case CONFIGURATION:
        bodyEncoder = configurationEntryEncoder;
        length +=
            serializeConfigurationEntry(
                buffer, entryOffset, (ConfigurationEntry<ConfigurationEntryMember>) entry);
        break;
      case INITIALIZE:
        bodyEncoder = initialEntryEncoder;
        length += serializeInitialEntry(buffer, entryOffset, entry);
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
      case ConfigurationEntryDecoder.TEMPLATE_ID:
        return deserializeConfigurationEntry(buffer, entryOffset);
      case InitialEntryDecoder.TEMPLATE_ID:
        return deserializeInitialEntry(buffer, entryOffset);
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

  int computeConfigurationEntryLength(final ConfigurationEntry<ConfigurationEntryMember> entry) {
    int length =
        headerEncoder.encodedLength()
            + configurationEntryEncoder.sbeBlockLength()
            + RaftMembersEncoder.sbeHeaderSize();
    for (final ConfigurationEntryMember member : entry.members()) {
      length +=
          RaftMembersEncoder.sbeBlockLength()
              + RaftMembersEncoder.memberIdHeaderLength()
              + member.memberIdBuffer().capacity();
    }

    return length;
  }

  int serializeConfigurationEntry(
      final MutableDirectBuffer buffer,
      final int offset,
      final ConfigurationEntry<ConfigurationEntryMember> entry) {
    configurationEntryEncoder.wrap(buffer, offset).term(entry.term()).timestamp(entry.timestamp());
    int length = configurationEntryEncoder.sbeBlockLength() + RaftMembersEncoder.sbeHeaderSize();
    final RaftMembersEncoder raftMembersEncoder =
        configurationEntryEncoder.raftMembersCount(entry.members().size());

    for (final ConfigurationEntryMember member : entry.members()) {
      final DirectBuffer memberIdBuffer = member.memberIdBuffer();
      raftMembersEncoder
          .next()
          .hash(member.hash())
          .updated(member.getLastUpdated().toEpochMilli())
          .role(member.role())
          .putMemberId(memberIdBuffer, 0, memberIdBuffer.capacity());
      length +=
          RaftMembersEncoder.sbeBlockLength()
              + RaftMembersEncoder.memberIdHeaderLength()
              + member.memberIdBuffer().capacity();
    }

    return length;
  }

  ConfigurationEntry<RaftMember> deserializeConfigurationEntry(
      final DirectBuffer buffer, final int offset) {
    configurationEntryDecoder.wrap(
        buffer, offset, headerDecoder.blockLength(), headerDecoder.version());

    final List<RaftMember> members = new ArrayList<>();
    final ConfigurationEntryDecoder.RaftMembersDecoder raftMembersDecoder =
        configurationEntryDecoder.raftMembers();

    while (raftMembersDecoder.hasNext()) {
      final ConfigurationEntryDecoder.RaftMembersDecoder raftMemberDecoder =
          raftMembersDecoder.next();
      members.add(
          new DefaultRaftMember(
              MemberId.from(raftMemberDecoder.memberId()),
              mapRole(raftMemberDecoder.role()),
              Instant.ofEpochMilli(raftMemberDecoder.updated())));
    }

    return new io.atomix.storage.journal.ConfigurationEntry<>(
        configurationEntryDecoder.term(), configurationEntryDecoder.timestamp(), members);
  }

  int computeInitialEntryLength() {
    return headerEncoder.encodedLength() + initialEntryEncoder.sbeBlockLength();
  }

  int serializeInitialEntry(final MutableDirectBuffer buffer, final int offset, final Entry entry) {
    initialEntryEncoder.wrap(buffer, offset).term(entry.term()).timestamp(entry.timestamp());
    return initialEntryEncoder.sbeBlockLength();
  }

  InitialEntry deserializeInitialEntry(final DirectBuffer buffer, final int offset) {
    initialEntryDecoder.wrap(buffer, offset, headerDecoder.blockLength(), headerDecoder.version());
    return new InitialEntry(initialEntryDecoder.term(), initialEntryDecoder.timestamp());
  }

  public static Role mapType(final Type type) {
    switch (type) {
      case INACTIVE:
        return Role.INACTIVE;
      case PASSIVE:
        return Role.PASSIVE;
      case PROMOTABLE:
        return Role.PROMOTABLE;
      case ACTIVE:
        return Role.ACTIVE;
      default:
      case BOOTSTRAP:
        return Role.NULL_VAL;
    }
  }

  private Type mapRole(final Role role) {
    switch (role) {
      case INACTIVE:
        return Type.INACTIVE;
      case PASSIVE:
        return Type.PASSIVE;
      case PROMOTABLE:
        return Type.PROMOTABLE;
      case ACTIVE:
        return Type.ACTIVE;
      case SBE_UNKNOWN:
      case NULL_VAL:
      default:
        return Type.INACTIVE;
    }
  }
}
