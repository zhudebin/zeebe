/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.logstreams.impl.log;

import io.atomix.raft.storage.log.entry.ApplicationEntryImpl;
import io.atomix.raft.zeebe.EntryValidator;
import io.atomix.raft.zeebe.ValidationResult;
import org.agrona.concurrent.UnsafeBuffer;

public class ZeebeEntryValidator implements EntryValidator {
  @Override
  public ValidationResult validateEntry(
      final ApplicationEntryImpl lastEntry, final ApplicationEntryImpl entry) {
    final UnsafeBuffer reader = new UnsafeBuffer(entry.data());
    long lastPosition = lastEntry != null ? lastEntry.highestAsqn() : -1;
    int offset = 0;

    do {
      final long position = LogEntryDescriptor.getPosition(reader, offset);
      if (lastPosition != -1 && position != lastPosition + 1) {
        return ValidationResult.failure(
            String.format(
                "Unexpected position %d was encountered after position %d when appending positions <%d, %d>.",
                position, lastPosition, entry.lowestAsqn(), entry.highestAsqn()));
      }
      lastPosition = position;

      offset += LogEntryDescriptor.getFragmentLength(reader, offset);
    } while (offset < reader.capacity());

    return ValidationResult.success();
  }
}
