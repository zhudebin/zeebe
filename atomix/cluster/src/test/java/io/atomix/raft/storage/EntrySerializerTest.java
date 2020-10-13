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
package io.atomix.raft.storage;

import static org.assertj.core.api.Assertions.assertThat;

import io.atomix.cluster.MemberId;
import io.atomix.raft.cluster.RaftMember;
import io.atomix.raft.cluster.RaftMember.Type;
import io.atomix.raft.cluster.impl.DefaultRaftMember;
import io.atomix.raft.storage.log.entry.ConfigurationEntry;
import io.atomix.raft.storage.log.entry.EntrySerializer;
import io.atomix.raft.storage.log.entry.ZeebeEntry;
import io.atomix.storage.journal.Indexed;
import io.atomix.storage.journal.RaftLogEntry;
import io.atomix.storage.protocol.EntryType;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Set;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

public class EntrySerializerTest {

  private final EntrySerializer serializer = new EntrySerializer();
  private final UnsafeBuffer raftLogEntryMemory = new UnsafeBuffer(ByteBuffer.allocateDirect(1024));
  private final UnsafeBuffer zbEntryMemory = new UnsafeBuffer(ByteBuffer.allocateDirect(1024));

  @Test
  public void shouldSerializeEntry() {
    final long term = 123;
    final long timestamp = 234;
    final long lowestPosition = 345;
    final long highestPosition = 456;
    final DirectBuffer record = new UnsafeBuffer(ByteBuffer.wrap("cenas".getBytes()));

    // serialize ZeebeEntry
    final ZeebeEntry zbEntry =
        new ZeebeEntry(term, timestamp, lowestPosition, highestPosition, record);
    final int zbEntryLength = serializer.serializeZeebeEntry(zbEntryMemory, 0, zbEntry);

    // serialize RaftLogEntry
    final RaftLogEntry entry =
        new RaftLogEntry(
            term, timestamp, EntryType.ZEEBE, new UnsafeBuffer(zbEntryMemory, 0, zbEntryLength));
    final int rfEntryLength = serializer.serializeRaftLogEntry(raftLogEntryMemory, 0, entry);

    // deserialize RaftLogEntry
    final RaftLogEntry deserializedEntry =
        serializer.deserializeRaftLogEntry(raftLogEntryMemory, 0);

    final ZeebeEntry deserializedZeebeEntry =
        serializer.deserializeZeebeEntry(deserializedEntry, 0);

    assertThat(zbEntry.highestPosition()).isEqualTo(deserializedZeebeEntry.highestPosition());
    assertThat(zbEntry.lowestPosition()).isEqualTo(deserializedZeebeEntry.lowestPosition());
    assertThat(zbEntry.data()).isEqualTo(deserializedZeebeEntry.data());

    final ZeebeEntry converted =
        serializer.asZeebeEntry(new Indexed<>(1, entry, entry.entry().capacity())).entry();
    assertThat(converted).isEqualTo(deserializedZeebeEntry);
  }

  @Test
  public void shouldSerializeConfigurationEntry() {
    final long term = 123;
    final long timestamp = 234;
    final Set<RaftMember> members =
        Set.of(
            new DefaultRaftMember(MemberId.from("foo"), Type.ACTIVE, Instant.now()),
            new DefaultRaftMember(MemberId.from("bar"), Type.PASSIVE, Instant.now()));

    // serialize config entry
    final ConfigurationEntry cfEntry = new ConfigurationEntry(term, timestamp, members);
    final int zbEntryLength = serializer.serializeConfigurationEntry(zbEntryMemory, 0, cfEntry);

    // serialize RaftLogEntry
    final RaftLogEntry entry =
        new RaftLogEntry(
            term,
            timestamp,
            EntryType.CONFIGURATION,
            new UnsafeBuffer(zbEntryMemory, 0, zbEntryLength));
    final int rfEntryLength = serializer.serializeRaftLogEntry(raftLogEntryMemory, 0, entry);

    // deserialize RaftLogEntry
    final RaftLogEntry deserializedEntry =
        serializer.deserializeRaftLogEntry(raftLogEntryMemory, 0);

    final ConfigurationEntry deserializedConfigEntry =
        serializer.deserializeConfigurationEntry(deserializedEntry, 0);

    assertThat(cfEntry.members()).isEqualTo(deserializedConfigEntry.members());

    final ConfigurationEntry converted =
        serializer.asConfigurationEntry(new Indexed<>(1, entry, entry.entry().capacity())).entry();
    assertThat(converted).isEqualTo(deserializedConfigEntry);
  }
}
