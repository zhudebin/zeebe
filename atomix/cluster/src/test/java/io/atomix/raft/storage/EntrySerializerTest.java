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
import io.atomix.raft.storage.log.entry.EntrySerializer;
import io.atomix.storage.journal.ConfigurationEntry;
import io.atomix.storage.journal.Entry;
import io.atomix.storage.journal.ZeebeEntry;
import io.atomix.storage.protocol.EntryType;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

public class EntrySerializerTest {

  private final EntrySerializer serializer = new EntrySerializer();
  private final UnsafeBuffer serializationBuffer =
      new UnsafeBuffer(ByteBuffer.allocateDirect(1024));
  private final UnsafeBuffer zbEntryMemory = new UnsafeBuffer(ByteBuffer.allocateDirect(1024));

  @Test
  public void shouldSerializeEntry() {
    final long term = 123;
    final long timestamp = 234;
    final long lowestPosition = 345;
    final long highestPosition = 456;
    final DirectBuffer record = new UnsafeBuffer(ByteBuffer.wrap("cenas".getBytes()));

    final ZeebeEntry entry =
        new ZeebeEntry(term, timestamp, lowestPosition, highestPosition, record);
    final int entryLength = serializer.serializeEntry(serializationBuffer, 0, entry);
    assertThat(entryLength).isEqualTo(serializer.computeEntryLength(entry));

    final Entry deserializedEntry = serializer.deserializeEntry(serializationBuffer, 0);
    assertThat(deserializedEntry.type()).isEqualTo(EntryType.ZEEBE);

    final ZeebeEntry deserializedZeebeEntry = (ZeebeEntry) deserializedEntry;
    assertThat(deserializedZeebeEntry).isEqualTo(entry);
  }

  @Test
  public void shouldSerializeConfigurationEntry() {
    final long term = 123;
    final long timestamp = 234;
    final List<RaftMember> members =
        List.of(
            new DefaultRaftMember(
                MemberId.from("foo"), Type.ACTIVE, Instant.now().truncatedTo(ChronoUnit.MILLIS)),
            new DefaultRaftMember(
                MemberId.from("bar"), Type.PASSIVE, Instant.now().truncatedTo(ChronoUnit.MILLIS)));

    final ConfigurationEntry<RaftMember> entry = new ConfigurationEntry<>(term, timestamp, members);
    final int entryLength = serializer.serializeEntry(serializationBuffer, 0, entry);
    assertThat(entryLength).isEqualTo(serializer.computeEntryLength(entry));

    final Entry deserializedEntry = serializer.deserializeEntry(serializationBuffer, 0);
    assertThat(deserializedEntry.type()).isEqualTo(EntryType.CONFIGURATION);

    final ConfigurationEntry<RaftMember> deserializedConfigEntry =
        (ConfigurationEntry<RaftMember>) deserializedEntry;
    assertThat(deserializedConfigEntry).isEqualTo(entry);
  }
}
