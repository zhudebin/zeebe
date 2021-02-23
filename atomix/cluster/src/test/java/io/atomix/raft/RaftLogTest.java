package io.atomix.raft;

import static org.assertj.core.api.Assertions.assertThat;

import io.atomix.raft.storage.log.RaftLog;
import io.atomix.raft.storage.log.RaftLogReader;
import io.atomix.raft.storage.log.RaftLogReader.Mode;
import io.atomix.raft.storage.log.entry.ApplicationEntryImpl;
import io.atomix.raft.storage.log.entry.RaftEntry;
import io.atomix.raft.zeebe.ZeebeEntry;
import io.zeebe.util.buffer.BufferUtil;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RaftLogTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private RaftLogReader reader;
  private RaftLog log;

  @Before
  public void setup() throws IOException {
    final File directory = temporaryFolder.newFolder();
    log = RaftLog.builder().withDirectory(directory).withJournalIndexDensity(5).build();
    reader = log.openReader(0, Mode.ALL);
  }

  @Test
  public void test() {
    final ByteBuffer data = ByteBuffer.wrap(BufferUtil.wrapString("test").byteArray());

    log.append(new ZeebeEntry(99, 1, 55, 57, data));
    log.append(new ZeebeEntry(11, 1, 33, 57, data));

    RaftEntry entry = reader.next();
    assertThat(entry.index()).isEqualTo(1);
    assertThat(entry.term()).isEqualTo(99);
    assertThat(entry.asqn()).isEqualTo(55);
    final ApplicationEntryImpl applicationEntry = entry.asApplicationEntry();
    assertThat(applicationEntry.lowestAsqn()).isEqualTo(55);
    assertThat(applicationEntry.highestAsqn()).isEqualTo(57);
    assertThat(BufferUtil.bufferAsString(applicationEntry.data())).isEqualTo("test");

    entry = reader.next();
    assertThat(entry.index()).isEqualTo(2);
    assertThat(entry.term()).isEqualTo(11);
    assertThat(entry.asqn()).isEqualTo(33);
  }
}
