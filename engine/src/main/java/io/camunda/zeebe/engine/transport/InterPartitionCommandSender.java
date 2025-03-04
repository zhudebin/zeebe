/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.transport;

import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.util.buffer.BufferWriter;

/**
 * Supports sending arbitrary commands to another partition. Sending may be unreliable and fail
 * silently, it is up to the caller to detect this and retry.
 */
public interface InterPartitionCommandSender {
  void sendCommand(
      final int receiverPartitionId,
      final ValueType valueType,
      final Intent intent,
      final BufferWriter command);

  /**
   * Uses the given record key when writing the command. Otherwise, behaves like {@link
   * InterPartitionCommandSender#sendCommand}
   *
   * @deprecated This is only available for compatability with deployment distribution.
   * @param recordKey Record key to use when writing the command. Ignored if null.
   */
  @Deprecated(forRemoval = true)
  void sendCommand(
      final int receiverPartitionId,
      final ValueType valueType,
      final Intent intent,
      final Long recordKey,
      final BufferWriter command);
}
