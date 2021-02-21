package io.zeebe.logstreams.storage.atomix;

import static org.assertj.core.api.Assertions.assertThat;

import io.atomix.raft.storage.log.RaftLog;
import io.atomix.raft.zeebe.ZeebeEntry;
import io.atomix.raft.zeebe.ZeebeLogAppender;
import io.atomix.storage.journal.Indexed;
import io.zeebe.logstreams.storage.LogStorage.AppendListener;
import io.zeebe.logstreams.storage.LogStorageReader;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class AtomixLogStorageReaderTest {
  private RaftLog log;
  private AtomixLogStorage logStorage;
  private Appender appender;

  @BeforeEach
  void beforeEach(final @TempDir Path tempDir) {
    final File directory = tempDir.resolve("log").toFile();

    log = RaftLog.builder().withDirectory(directory).build();
    appender = new Appender();
    logStorage = new AtomixLogStorage(log::openReader, () -> Optional.of(appender));
  }

  @AfterEach
  void afterEach() {
    CloseHelper.quietClose(log);
  }

  @Test
  void shouldBeInitializedAtFirstBlock() {
    // given
    appendIntegerBlock(1);
    appendIntegerBlock(2);

    // when
    try (final LogStorageReader reader = logStorage.newReader()) {
      // then
      assertThat(reader).hasNext();
      assertThat(reader.next()).isEqualTo(mapIntegerToBuffer(1));
    }
  }

  @Test
  void shouldSeekToFirstBlock() {
    // given
    appendIntegerBlock(1);
    appendIntegerBlock(2);

    // when
    try (final LogStorageReader reader = logStorage.newReader()) {
      reader.seek(Long.MIN_VALUE);

      // then
      assertThat(reader).hasNext();
      assertThat(reader.next()).isEqualTo(mapIntegerToBuffer(1));
    }
  }

  @Test
  void shouldSeekToLastBlock() {
    // given
    appendIntegerBlock(1);
    appendIntegerBlock(2);

    // when
    try (final LogStorageReader reader = logStorage.newReader()) {
      reader.seek(Long.MAX_VALUE);

      // then
      assertThat(reader).hasNext();
      assertThat(reader.next()).isEqualTo(mapIntegerToBuffer(2));
    }
  }

  @Test
  void shouldSeekToHighestPositionThatIsLessThanTheGivenOne() {
    // given
    appendIntegerBlock(1);
    appendIntegerBlock(3);

    // when
    try (final LogStorageReader reader = logStorage.newReader()) {
      reader.seek(2);

      // then
      assertThat(reader).hasNext();
      assertThat(reader.next()).isEqualTo(mapIntegerToBuffer(1));
    }
  }

  @Test
  void shouldSeekToBlockContainingPosition() {
    // given
    appendIntegerBlock(1);
    appendIntegerBlock(2, 4, 2);
    appendIntegerBlock(5);

    // when
    try (final LogStorageReader reader = logStorage.newReader()) {
      reader.seek(3);

      // then
      assertThat(reader).hasNext();
      assertThat(reader.next()).isEqualTo(mapIntegerToBuffer(2));
    }
  }

  @Test
  void shouldNotHaveNextIfEndIsReached() {
    // given
    appendIntegerBlock(1);
    appendIntegerBlock(2);

    // when
    try (final LogStorageReader reader = logStorage.newReader()) {
      reader.seek(2);
      reader.next();

      // then
      assertThat(reader.hasNext()).isFalse();
    }
  }

  @Test
  void shouldNotHaveNextIfEmpty() {
    // given an empty log
    // when
    try (final LogStorageReader reader = logStorage.newReader()) {
      // then
      assertThat(reader.hasNext()).isFalse();
    }
  }

  @Test
  void shouldHaveNextAfterNewBlockAppended() {
    // given an empty log
    // when
    try (final LogStorageReader reader = logStorage.newReader()) {
      appendIntegerBlock(1);

      // then
      assertThat(reader).hasNext();
      assertThat(reader.next()).isEqualTo(mapIntegerToBuffer(1));
    }
  }

  private void appendIntegerBlock(final int positionAndValue) {
    appendIntegerBlock(positionAndValue, positionAndValue, positionAndValue);
  }

  private void appendIntegerBlock(
      final long lowestPosition, final long highestPosition, final int value) {
    logStorage.append(
        lowestPosition,
        highestPosition,
        mapIntegerToBuffer(value).byteBuffer(),
        new AppendListener() {});
  }

  private DirectBuffer mapIntegerToBuffer(final int value) {
    return new UnsafeBuffer(ByteBuffer.allocateDirect(Integer.BYTES).putInt(0, value));
  }

  private final class Appender implements ZeebeLogAppender {
    @Override
    public void appendEntry(
        final long lowestPosition,
        final long highestPosition,
        final ByteBuffer data,
        final AppendListener appendListener) {
      final ZeebeEntry entry =
          new ZeebeEntry(1, System.currentTimeMillis(), lowestPosition, highestPosition, data);
      final Indexed<ZeebeEntry> indexedEntry = log.append(entry);

      appendListener.onWrite(indexedEntry);
      log.setCommitIndex(indexedEntry.index());
      appendListener.onCommit(indexedEntry);
    }
  }
}
