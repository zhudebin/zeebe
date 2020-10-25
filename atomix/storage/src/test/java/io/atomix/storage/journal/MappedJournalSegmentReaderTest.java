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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Stopwatch;
import io.atomix.storage.StorageLevel;
import io.atomix.storage.journal.JournalReader.Mode;
import io.atomix.storage.journal.index.SparseJournalIndex;
import io.zeebe.util.buffer.BufferUtil;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MappedJournalSegmentReaderTest {
  private static final ZeebeEntry ENTRY = new ZeebeEntry(1, 1, 1, 1, BufferUtil.wrapString("foo"));

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final TestJournalSerde serde = new TestJournalSerde();
  private final int entrySize = serde.computeEntryLength(ENTRY) + Integer.BYTES + Integer.BYTES;
  private File directory;

  @Before
  public void setUp() throws IOException {
    directory = temporaryFolder.newFolder();
  }

  @Test
  public void dumbSpeedTest() {
    // given
    final int entriesPerSegment = 7;
    final SparseJournalIndex journalIndex = new SparseJournalIndex(5);
    try (final SegmentedJournal journal = createJournal(entriesPerSegment, journalIndex)) {
      final SegmentedJournalWriter writer = journal.writer();
      final SegmentedJournalReader reader = journal.openReader(1, Mode.ALL);

      final var expectedEntryCount = entriesPerSegment * 1000;
      for (int i = 0; i < expectedEntryCount; i++) {
        writer.append(
            new ZeebeEntry(
                1,
                1,
                1,
                1,
                new UnsafeBuffer(
                    ByteBuffer.allocate(Integer.BYTES)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putInt(0, i))));
      }

      // when
      final var entries = new ArrayList<Entry>();
      final var watch = Stopwatch.createStarted();
      while (reader.hasNext()) {
        entries.add(reader.next().entry());
      }
      watch.stop();

      // then
      System.out.println("Reading took: " + watch.elapsed());
    }
  }

  @Test
  public void shouldReadEventsOnAllSegments() {
    // given
    final int entriesPerSegment = 7;
    final SparseJournalIndex journalIndex = new SparseJournalIndex(5);
    try (final SegmentedJournal journal = createJournal(entriesPerSegment, journalIndex)) {
      final SegmentedJournalWriter writer = journal.writer();
      final SegmentedJournalReader reader = journal.openReader(1, Mode.ALL);

      final var expectedEntryCount = entriesPerSegment * 3;
      for (int i = 0; i < expectedEntryCount; i++) {
        writer.append(
            new ZeebeEntry(
                1,
                1,
                1,
                1,
                new UnsafeBuffer(
                    ByteBuffer.allocate(Integer.BYTES)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putInt(0, i))));
      }

      // when
      final var entries = new ArrayList<Entry>();
      while (reader.hasNext()) {
        entries.add(reader.next().entry());
      }

      // then
      assertThat(
              entries.stream()
                  .map(ZeebeEntry.class::cast)
                  .map(e -> e.data().getInt(0, ByteOrder.LITTLE_ENDIAN)))
          .hasSize(expectedEntryCount)
          .isEqualTo(IntStream.range(0, expectedEntryCount).boxed().collect(Collectors.toList()));
    }
  }

  @Test
  public void shouldResetBackwardsCorrectlyWhenUsingSameIndex() {
    // given
    final int entriesPerSegment = 7;
    final SparseJournalIndex journalIndex = new SparseJournalIndex(5);
    try (final SegmentedJournal journal = createJournal(entriesPerSegment, journalIndex)) {
      final SegmentedJournalWriter writer = journal.writer();
      final SegmentedJournalReader reader = journal.openReader(1, Mode.ALL);

      for (int i = 1; i <= entriesPerSegment; i++) {
        writer.append(ENTRY);
      }

      writer.append(ENTRY);
      final Indexed<ZeebeEntry> previousEntry = writer.append(ENTRY);
      final Indexed<ZeebeEntry> currentEntry = writer.append(ENTRY);

      reader.reset(currentEntry.index());
      assertThat(reader.hasNext()).isTrue();
      assertThat(reader.next().index()).isEqualTo(currentEntry.index());

      reader.reset(previousEntry.index());
      assertThat(reader.hasNext()).isTrue();
      assertThat(reader.next().index()).isEqualTo(previousEntry.index());
    }
  }

  private SegmentedJournal createJournal(
      final int entriesPerSegment, final SparseJournalIndex journalIndex) {
    final int maxSegmentSize =
        (entriesPerSegment * entrySize * 100) + JournalSegmentDescriptor.BYTES;
    return SegmentedJournal.<Integer>builder()
        .withName("test")
        .withDirectory(directory)
        .withSerde(TestJournalSerde::new)
        .withStorageLevel(StorageLevel.MAPPED)
        .withMaxEntrySize(entrySize * 2)
        .withMaxSegmentSize(maxSegmentSize)
        .withJournalIndexFactory(() -> journalIndex)
        .build();
  }
}
