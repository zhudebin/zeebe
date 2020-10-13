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
import io.atomix.storage.journal.Indexed;
import io.atomix.storage.journal.JournalSerde;
import io.atomix.storage.journal.RaftLogEntry;
import io.atomix.storage.protocol.ConfigurationDecoder;
import io.atomix.storage.protocol.ConfigurationDecoder.RaftMembersDecoder;
import io.atomix.storage.protocol.ConfigurationEncoder;
import io.atomix.storage.protocol.ConfigurationEncoder.RaftMembersEncoder;
import io.atomix.storage.protocol.EntryDecoder;
import io.atomix.storage.protocol.EntryEncoder;
import io.atomix.storage.protocol.MessageHeaderDecoder;
import io.atomix.storage.protocol.MessageHeaderEncoder;
import io.atomix.storage.protocol.Role;
import io.atomix.storage.protocol.ZeebeDecoder;
import io.atomix.storage.protocol.ZeebeEncoder;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class EntrySerializer implements JournalSerde {

  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final EntryEncoder rfEncoder = new EntryEncoder();
  private final EntryDecoder rfDecoder = new EntryDecoder();
  private final ZeebeEncoder zbEncoder = new ZeebeEncoder();
  private final ZeebeDecoder zbDecoder = new ZeebeDecoder();
  private final ConfigurationEncoder cfEncoder = new ConfigurationEncoder();
  private final ConfigurationDecoder cfDecoder = new ConfigurationDecoder();

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
    rfDecoder.getEntry(entryBuffer, 0, entryBuffer.capacity());

    return new RaftLogEntry(
        rfDecoder.term(), rfDecoder.timestamp(), rfDecoder.entryType(), entryBuffer);
  }

  public int serializeZeebeEntry(
      final MutableDirectBuffer buffer, final int offset, final ZeebeEntry entry) {
    headerEncoder
        .wrap(buffer, offset)
        .blockLength(zbEncoder.sbeBlockLength())
        .templateId(zbEncoder.sbeTemplateId())
        .schemaId(zbEncoder.sbeSchemaId())
        .version(zbEncoder.sbeSchemaVersion());

    zbEncoder.wrap(buffer, offset + headerEncoder.encodedLength());
    zbEncoder
        .lowestPosition(entry.lowestPosition())
        .highestPosition(entry.highestPosition())
        .putData(entry.data(), 0, entry.data().capacity());

    return headerEncoder.encodedLength() + zbEncoder.encodedLength();
  }

  public ZeebeEntry deserializeZeebeEntry(final RaftLogEntry entry, final int offset) {
    headerDecoder.wrap(entry.entry(), offset);
    zbDecoder.wrap(
        entry.entry(),
        offset + headerDecoder.encodedLength(),
        headerDecoder.blockLength(),
        headerDecoder.version());

    final long lowestPosition = zbDecoder.lowestPosition();
    final long highestPosition = zbDecoder.highestPosition();
    final UnsafeBuffer dataBuffer = new UnsafeBuffer();
    zbDecoder.wrapData(dataBuffer);

    return new ZeebeEntry(
        entry.term(), entry.timestamp(), lowestPosition, highestPosition, dataBuffer);
  }

  public int serializeConfigurationEntry(
      final MutableDirectBuffer buffer, final int offset, final ConfigurationEntry entry) {
    headerEncoder
        .wrap(buffer, offset)
        .blockLength(cfEncoder.sbeBlockLength())
        .templateId(cfEncoder.sbeTemplateId())
        .schemaId(cfEncoder.sbeSchemaId())
        .version(cfEncoder.sbeSchemaVersion());

    cfEncoder.wrap(buffer, offset + headerEncoder.encodedLength());
    final RaftMembersEncoder rmEncoder = cfEncoder.raftMembersCount(entry.members().size());

    for (final RaftMember member : entry.members()) {
      rmEncoder
          .next()
          .hash(member.hash())
          .updated(member.getLastUpdated().toEpochMilli())
          .memberId(member.memberId().id())
          .role(mapType(member.getType()));
    }

    return headerEncoder.encodedLength() + cfEncoder.encodedLength();
  }

  public ConfigurationEntry deserializeConfigurationEntry(
      final RaftLogEntry entry, final int offset) {
    final Set<RaftMember> members = new HashSet<>();
    final DirectBuffer entryBuffer = entry.entry();
    headerDecoder.wrap(entryBuffer, offset);
    cfDecoder.wrap(
        entryBuffer,
        offset + headerDecoder.encodedLength(),
        headerDecoder.blockLength(),
        headerDecoder.version());

    final RaftMembersDecoder rmDecoder = cfDecoder.raftMembers();
    while (rmDecoder.hasNext()) {
      final RaftMembersDecoder memberDecoder = rmDecoder.next();
      members.add(
          new DefaultRaftMember(
              MemberId.from(memberDecoder.memberId()),
              mapRole(memberDecoder.role()),
              Instant.ofEpochMilli(memberDecoder.updated())));
    }

    return new ConfigurationEntry(entry.term(), entry.timestamp(), members);
  }

  public Indexed<ZeebeEntry> asZeebeEntry(final Indexed<RaftLogEntry> indexed) {
    final RaftLogEntry entry = indexed.entry();
    return new Indexed<>(
        indexed.index(), deserializeZeebeEntry(entry, 0), entry.entry().capacity());
  }

  public Indexed<ConfigurationEntry> asConfigurationEntry(final Indexed<RaftLogEntry> indexed) {
    final RaftLogEntry entry = indexed.entry();
    return new Indexed<>(
        indexed.index(), deserializeConfigurationEntry(entry, 0), entry.entry().capacity());
  }

  public Indexed<InitializeEntry> asInitializeEntry(final Indexed<RaftLogEntry> indexed) {
    final RaftLogEntry entry = indexed.entry();
    return new Indexed<>(indexed.index(), new InitializeEntry(entry.term(), entry.timestamp()), 0);
  }

  public <E extends EntryValue> RaftLogEntry asRaftLogEntry(
      final E entry, final MutableDirectBuffer buffer, final int offset) {
    final int length = entry.serialize(this, buffer, offset);
    return new RaftLogEntry(
        entry.term(), entry.timestamp(), entry.type(), new UnsafeBuffer(buffer, offset, length));
  }

  private Role mapType(final Type type) {
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
