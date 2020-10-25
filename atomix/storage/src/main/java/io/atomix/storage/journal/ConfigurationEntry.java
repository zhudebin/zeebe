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
import java.util.Collection;
import java.util.Objects;

public class ConfigurationEntry<T extends ConfigurationEntryMember> implements Entry {
  private final Collection<T> members;
  private final long term;
  private final long timestamp;

  public ConfigurationEntry(final long term, final long timestamp, final Collection<T> members) {
    this.term = term;
    this.timestamp = timestamp;
    this.members = members;
  }

  /**
   * Returns the members.
   *
   * @return The members.
   */
  public Collection<T> members() {
    return members;
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
    return EntryType.CONFIGURATION;
  }

  @Override
  public int hashCode() {
    return Objects.hash(members, term, timestamp);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ConfigurationEntry<T> that = (ConfigurationEntry<T>) o;
    return term == that.term && timestamp == that.timestamp && members.equals(that.members);
  }

  @Override
  public String toString() {
    return "ConfigurationEntry{"
        + "members="
        + members
        + ", term="
        + term
        + ", timestamp="
        + timestamp
        + '}';
  }
}
