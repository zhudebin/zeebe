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
package io.atomix.raft.storage.log;

import io.atomix.cluster.MemberId;
import io.atomix.raft.cluster.RaftMember;
import io.atomix.raft.cluster.impl.DefaultRaftMember;
import io.atomix.raft.storage.log.entry.ApplicationEntry;
import io.atomix.raft.storage.log.entry.ApplicationEntryDecoder;
import io.atomix.raft.storage.log.entry.ApplicationEntryEncoder;
import io.atomix.raft.storage.log.entry.ConfigurationEntry;
import io.atomix.raft.storage.log.entry.ConfigurationEntryDecoder;
import io.atomix.raft.storage.log.entry.ConfigurationEntryDecoder.RaftMemberDecoder;
import io.atomix.raft.storage.log.entry.ConfigurationEntryEncoder;
import io.atomix.raft.storage.log.entry.EntryType;
import io.atomix.raft.storage.log.entry.InitialEntry;
import io.atomix.raft.storage.log.entry.MemberType;
import io.atomix.raft.storage.log.entry.MessageHeaderDecoder;
import io.atomix.raft.storage.log.entry.MessageHeaderEncoder;
import io.atomix.raft.storage.log.entry.RaftEntry;
import io.atomix.raft.storage.log.entry.RaftLogEntry;
import io.atomix.raft.storage.log.entry.RaftLogEntryDecoder;
import io.atomix.raft.storage.log.entry.RaftLogEntryEncoder;
import io.zeebe.util.StringUtil;
import io.zeebe.util.buffer.BufferUtil;
import java.time.Instant;
import java.util.ArrayList;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class RaftEntrySBESerializer implements RaftEntrySerializer {
  final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  final RaftLogEntryEncoder raftLogEntryEncoder = new RaftLogEntryEncoder();
  final ApplicationEntryEncoder applicationEntryEncoder = new ApplicationEntryEncoder();
  final ConfigurationEntryEncoder configurationEntryEncoder = new ConfigurationEntryEncoder();

  final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  final RaftLogEntryDecoder raftLogEntryDecoder = new RaftLogEntryDecoder();
  final ApplicationEntryDecoder applicationEntryDecoder = new ApplicationEntryDecoder();
  final ConfigurationEntryDecoder configurationEntryDecoder = new ConfigurationEntryDecoder();

  @Override
  public int writeApplicationEntry(
      final long term,
      final ApplicationEntry entry,
      final MutableDirectBuffer buffer,
      final int offset) {

    final int entryOffset = writeRaftFrame(term, EntryType.ApplicationEntry, buffer, offset);

    headerEncoder
        .wrap(buffer, offset + entryOffset)
        .blockLength(applicationEntryEncoder.sbeBlockLength())
        .templateId(applicationEntryEncoder.sbeTemplateId())
        .schemaId(applicationEntryEncoder.sbeSchemaId())
        .version(applicationEntryEncoder.sbeSchemaVersion());
    applicationEntryEncoder.wrap(buffer, offset + entryOffset + headerEncoder.encodedLength());
    applicationEntryEncoder
        .lowestAsqn(entry.lowestPosition())
        .highestAsqn(entry.highestPosition())
        .putApplicationData(new UnsafeBuffer(entry.data()), 0, entry.data().capacity());

    return entryOffset + headerEncoder.encodedLength() + applicationEntryEncoder.encodedLength();
  }

  @Override
  public int writeInitialEntry(
      final long term,
      final InitialEntry entry,
      final MutableDirectBuffer buffer,
      final int offset) {

    return writeRaftFrame(term, EntryType.InitialEntry, buffer, offset);
  }

  @Override
  public int writeConfigurationEntry(
      final long term,
      final ConfigurationEntry entry,
      final MutableDirectBuffer buffer,
      final int offset) {
    final int entryOffset = writeRaftFrame(term, EntryType.ConfigurationEntry, buffer, offset);

    headerEncoder
        .wrap(buffer, offset + entryOffset)
        .blockLength(configurationEntryEncoder.sbeBlockLength())
        .templateId(configurationEntryEncoder.sbeTemplateId())
        .schemaId(configurationEntryEncoder.sbeSchemaId())
        .version(configurationEntryEncoder.sbeSchemaVersion());

    configurationEntryEncoder.wrap(buffer, offset + entryOffset + headerEncoder.encodedLength());

    configurationEntryEncoder.timestamp(entry.timestamp());

    final var raftMemberEncoder = configurationEntryEncoder.raftMemberCount(entry.members().size());
    for (final RaftMember member : entry.members()) {
      final var memberId = StringUtil.getBytes(member.memberId().id());
      raftMemberEncoder
          .next()
          .type(getSBEType(member.getType()))
          .updated(member.getLastUpdated().toEpochMilli())
          .putMemberId(memberId, 0, memberId.length);
    }

    return entryOffset + headerEncoder.encodedLength() + configurationEntryEncoder.encodedLength();
  }

  @Override
  public RaftLogEntry readRaftLogEntry(final DirectBuffer buffer) {
    headerDecoder.wrap(buffer, 0);
    raftLogEntryDecoder.wrap(
        buffer,
        headerDecoder.encodedLength(),
        headerDecoder.blockLength(),
        headerDecoder.version());
    final long term = raftLogEntryDecoder.term();
    final EntryType type = raftLogEntryDecoder.type();

    final RaftEntry entry;
    final int entryOffset = headerDecoder.encodedLength() + raftLogEntryDecoder.encodedLength();

    switch (type) {
      case ApplicationEntry:
        headerDecoder.wrap(buffer, entryOffset);
        entry = readApplicationEntry(buffer, entryOffset);
        break;
      case ConfigurationEntry:
        headerDecoder.wrap(buffer, entryOffset);
        entry = readConfigurationEntry(buffer, entryOffset);
        break;
      case InitialEntry:
        entry = new InitialEntry();
        break;
      default:
        throw new IllegalStateException("Unexpected entry type " + type);
    }

    return new RaftLogEntry(term, entry);
  }

  private int writeRaftFrame(
      final long term,
      final EntryType entryType,
      final MutableDirectBuffer buffer,
      final int offset) {
    headerEncoder
        .wrap(buffer, offset)
        .blockLength(raftLogEntryEncoder.sbeBlockLength())
        .templateId(raftLogEntryEncoder.sbeTemplateId())
        .schemaId(raftLogEntryEncoder.sbeSchemaId())
        .version(raftLogEntryEncoder.sbeSchemaVersion());
    raftLogEntryEncoder.wrap(buffer, offset + headerEncoder.encodedLength());
    raftLogEntryEncoder.term(term);
    raftLogEntryEncoder.type(entryType);

    return headerEncoder.encodedLength() + raftLogEntryEncoder.encodedLength();
  }

  private MemberType getSBEType(final RaftMember.Type type) {
    switch (type) {
      case ACTIVE:
        return MemberType.ACTIVE;
      case PASSIVE:
        return MemberType.PASSIVE;
      case INACTIVE:
        return MemberType.INACTIVE;
      case PROMOTABLE:
        return MemberType.PROMOTABLE;
      default:
        throw new IllegalStateException("Unexpected member type");
    }
  }

  private RaftMember.Type getRaftMemberType(final MemberType type) {
    switch (type) {
      case ACTIVE:
        return RaftMember.Type.ACTIVE;
      case PASSIVE:
        return RaftMember.Type.PASSIVE;
      case INACTIVE:
        return RaftMember.Type.INACTIVE;
      case PROMOTABLE:
        return RaftMember.Type.PROMOTABLE;
      default:
        throw new IllegalStateException("Unexpected member type " + type);
    }
  }

  private ApplicationEntry readApplicationEntry(final DirectBuffer buffer, final int entryOffset) {
    applicationEntryDecoder.wrap(
        buffer,
        entryOffset + headerDecoder.encodedLength(),
        headerDecoder.blockLength(),
        headerDecoder.version());

    final DirectBuffer data = new UnsafeBuffer();
    applicationEntryDecoder.wrapApplicationData(data);

    return new ApplicationEntry(
        applicationEntryDecoder.lowestAsqn(), applicationEntryDecoder.highestAsqn(), data);
  }

  private ConfigurationEntry readConfigurationEntry(
      final DirectBuffer buffer, final int entryOffset) {

    configurationEntryDecoder.wrap(
        buffer,
        entryOffset + headerDecoder.encodedLength(),
        headerDecoder.blockLength(),
        headerDecoder.version());

    final long timestamp = configurationEntryDecoder.timestamp();

    final RaftMemberDecoder memberDecoder = configurationEntryDecoder.raftMember();
    final ArrayList<RaftMember> members = new ArrayList<>(memberDecoder.count());
    for (final RaftMemberDecoder member : memberDecoder) {
      final RaftMember.Type type = getRaftMemberType(member.type());
      final Instant updated = Instant.ofEpochMilli(member.updated());
      final DirectBuffer memberIdBuffer = new UnsafeBuffer();
      member.wrapMemberId(memberIdBuffer);
      final String memberId = BufferUtil.bufferAsString(memberIdBuffer);
      members.add(new DefaultRaftMember(MemberId.from(memberId), type, updated));
    }

    return new ConfigurationEntry(timestamp, members);
  }
}
