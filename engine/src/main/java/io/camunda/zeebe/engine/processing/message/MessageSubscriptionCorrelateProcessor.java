/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.message;

import io.camunda.zeebe.engine.api.TypedRecord;
import io.camunda.zeebe.engine.processing.message.command.SubscriptionCommandSender;
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecordProcessor;
import io.camunda.zeebe.engine.processing.streamprocessor.sideeffect.SideEffectProducer;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.LegacyTypedResponseWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.LegacyTypedStreamWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.StateWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedRejectionWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.Writers;
import io.camunda.zeebe.engine.state.immutable.MessageState;
import io.camunda.zeebe.engine.state.immutable.MessageSubscriptionState;
import io.camunda.zeebe.engine.state.message.MessageSubscription;
import io.camunda.zeebe.protocol.impl.record.value.message.MessageSubscriptionRecord;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.intent.MessageSubscriptionIntent;
import io.camunda.zeebe.util.buffer.BufferUtil;
import java.util.function.Consumer;

public final class MessageSubscriptionCorrelateProcessor
    implements TypedRecordProcessor<MessageSubscriptionRecord> {

  private static final String NO_SUBSCRIPTION_FOUND_MESSAGE =
      "Expected to correlate subscription for element with key '%d' and message name '%s', "
          + "but no such message subscription exists";

  private final MessageSubscriptionState subscriptionState;
  private final MessageCorrelator messageCorrelator;
  private final StateWriter stateWriter;
  private final TypedRejectionWriter rejectionWriter;

  public MessageSubscriptionCorrelateProcessor(
      final MessageState messageState,
      final MessageSubscriptionState subscriptionState,
      final SubscriptionCommandSender commandSender,
      final Writers writers) {
    this.subscriptionState = subscriptionState;
    stateWriter = writers.state();
    rejectionWriter = writers.rejection();
    messageCorrelator = new MessageCorrelator(messageState, commandSender, stateWriter);
  }

  @Override
  public void processRecord(
      final TypedRecord<MessageSubscriptionRecord> record,
      final LegacyTypedResponseWriter responseWriter,
      final LegacyTypedStreamWriter streamWriter,
      final Consumer<SideEffectProducer> sideEffect) {

    final MessageSubscriptionRecord command = record.getValue();
    final MessageSubscription subscription =
        subscriptionState.get(command.getElementInstanceKey(), command.getMessageNameBuffer());

    if (subscription == null) {
      rejectCommand(record);
      return;
    }

    final var messageSubscription = subscription.getRecord();
    stateWriter.appendFollowUpEvent(
        subscription.getKey(), MessageSubscriptionIntent.CORRELATED, messageSubscription);

    if (!messageSubscription.isInterrupting()) {
      messageCorrelator.correlateNextMessage(
          subscription.getKey(), messageSubscription, sideEffect);
    }
  }

  private void rejectCommand(final TypedRecord<MessageSubscriptionRecord> record) {
    final var subscription = record.getValue();
    final var reason =
        String.format(
            NO_SUBSCRIPTION_FOUND_MESSAGE,
            subscription.getElementInstanceKey(),
            BufferUtil.bufferAsString(subscription.getMessageNameBuffer()));

    rejectionWriter.appendRejection(record, RejectionType.NOT_FOUND, reason);
  }
}
