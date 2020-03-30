/*
 * Copyright © 2020  camunda services GmbH (info@camunda.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package io.zeebe.engine.util;

import io.atomix.protocols.raft.zeebe.ZeebeEntry;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.logstreams.spi.LogStorageReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.LongConsumer;
import org.agrona.DirectBuffer;

class ListLogStrage implements LogStorage {

  private final List<ZeebeEntry> entries;
  private LongConsumer positionListener;

  public ListLogStrage() {
    entries = new CopyOnWriteArrayList<>();
  }

  public void setPositionListener(final LongConsumer positionListener) {
    this.positionListener = positionListener;
  }

  @Override
  public LogStorageReader newReader() {
    return new LogStorageReader() {
      @Override
      public boolean isEmpty() {
        return entries.isEmpty();
      }

      @Override
      public long read(final DirectBuffer readBuffer, final long address) {
        final var index = (int) (address - 1);

        if (index < 0 || index >= entries.size()) {
          return OP_RESULT_NO_DATA;
        }

        final var zeebeEntry = entries.get(index);
        final var data = zeebeEntry.data();
        readBuffer.wrap(data, data.position(), data.remaining());

        return address + 1;
      }

      @Override
      public long readLastBlock(final DirectBuffer readBuffer) {
        return read(readBuffer, entries.size());
      }

      @Override
      public long lookUpApproximateAddress(final long position) {

        if (position == Long.MIN_VALUE) {
          return entries.isEmpty() ? OP_RESULT_INVALID_ADDR : 1;
        }

        for (int idx = 0; idx < entries.size(); idx++) {
          final var zeebeEntry = entries.get(idx);
          if (zeebeEntry.lowestPosition() <= position && position <= zeebeEntry.highestPosition()) {
            return idx;
          }
        }

        return 1;
      }

      @Override
      public void close() {}
    };
  }

  @Override
  public void append(
      final long lowestPosition,
      final long highestPosition,
      final ByteBuffer blockBuffer,
      final AppendListener listener) {
    try {
      final var zeebeEntry =
          new ZeebeEntry(
              0, System.currentTimeMillis(), lowestPosition, highestPosition, blockBuffer);
      entries.add(zeebeEntry);
      listener.onWrite(entries.size());

      if (positionListener != null) {
        positionListener.accept(zeebeEntry.highestPosition());
      }
      listener.onCommit(entries.size());
    } catch (final Exception e) {
      listener.onWriteError(e);
    }
  }

  @Override
  public void open() throws IOException {}

  @Override
  public void close() {
    entries.clear();
  }

  @Override
  public boolean isOpen() {
    return true;
  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public void flush() throws Exception {}
}
