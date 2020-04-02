package io.zeebe.engine.processor.workflow.perf;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.assertj.core.internal.bytebuddy.utility.RandomString;
import org.junit.Before;
import org.junit.Test;

public class FileChannelTest {

  private static final int ITER_COUNT = 1000;
  private static final int ONE_MB = 1024 * 1024;

//  @Rule
//  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File file;

  private final int size = 4 * 1024 * 1024;

  @Before
  public void setup()
  {
    file = new File(FileChannelTest.class.getResource(".").getPath(), "file.txt");

  }

  @Test
  public void testRead() throws Exception {
    write(file.toPath());

    final ByteBuffer byteBuffer = ByteBuffer.allocate(size * 2);

    final var start = System.nanoTime();
    try (final var fileChannel = FileChannel
        .open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ,
            StandardOpenOption.WRITE)) {

      for (int i = 0; i < ITER_COUNT; i++)
      {
        fileChannel.read(byteBuffer);
        byteBuffer.clear();
      }
    }

    final var end = System.nanoTime();

    System.out.println("Diff " + ((end - start) / (1000 * 1000)));

  }

  private void write(final Path path) throws Exception {
    final var writeBuffer = ByteBuffer.allocate(ONE_MB);
    try (final var fileChannel = FileChannel
        .open(path, StandardOpenOption.CREATE, StandardOpenOption.READ,
            StandardOpenOption.WRITE)) {


      // write 100 MB
      for (int i = 0; i < 100; i++)
      {
        writeBuffer.clear();
        final var randomString = RandomString.make(ONE_MB);
        writeBuffer.put(randomString.getBytes());
        writeBuffer.flip();
        fileChannel.write(writeBuffer);
      }
    }
  }
}
