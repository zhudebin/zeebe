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

import io.atomix.storage.protocol.EntryType;
import java.util.Objects;
import org.agrona.DirectBuffer;

public class ZeebeEntry implements Entry {
  private final long term;
  private final long timestamp;
  private final long lowestPosition;
  private final long highestPosition;
  private final DirectBuffer dataBuffer;

  public ZeebeEntry(
      final long term,
      final long timestamp,
      final long lowestPosition,
      final long highestPosition,
      final DirectBuffer dataBuffer) {
    this.term = term;
    this.timestamp = timestamp;
    this.lowestPosition = lowestPosition;
    this.highestPosition = highestPosition;
    this.dataBuffer = dataBuffer;
  }

  @Override
  public long term() {
    return term;
  }

  @Override
  public long timestamp() {
    return timestamp;
  }

  @Override
  public EntryType type() {
    return EntryType.ZEEBE;
  }

  public long lowestPosition() {
    return lowestPosition;
  }

  public long highestPosition() {
    return highestPosition;
  }

  public DirectBuffer data() {
    return dataBuffer;
  }

  @Override
  public int hashCode() {
    return Objects.hash(term, timestamp, lowestPosition(), highestPosition(), data());
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ZeebeEntry that = (ZeebeEntry) o;
    return term == that.term
        && timestamp == that.timestamp
        && lowestPosition() == that.lowestPosition()
        && highestPosition() == that.highestPosition()
        && data().equals(that.data());
  }

  @Override
  public String toString() {
    return "ZeebeEntry{"
        + "term="
        + term
        + ", timestamp="
        + timestamp
        + ", lowestPosition="
        + lowestPosition
        + ", highestPosition="
        + highestPosition
        + ", dataBuffer.capacity()="
        + dataBuffer.capacity()
        + '}';
  }
}
