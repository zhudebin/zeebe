/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.zeebe.engine.processing.deployment.model.transformer;

import static io.zeebe.util.buffer.BufferUtil.wrapString;

import io.zeebe.el.Expression;
import io.zeebe.el.ExpressionLanguage;
import io.zeebe.engine.Loggers;
import io.zeebe.engine.processing.deployment.model.element.ExecutableProcess;
import io.zeebe.engine.processing.deployment.model.element.ExecutableUserTask;
import io.zeebe.engine.processing.deployment.model.transformation.ModelElementTransformer;
import io.zeebe.engine.processing.deployment.model.transformation.TransformContext;
import io.zeebe.model.bpmn.instance.UserTask;
import io.zeebe.model.bpmn.instance.zeebe.ZeebeFormDefinition;
import io.zeebe.model.bpmn.instance.zeebe.ZeebeHeader;
import io.zeebe.model.bpmn.instance.zeebe.ZeebeTaskDefinition;
import io.zeebe.model.bpmn.instance.zeebe.ZeebeTaskHeaders;
import io.zeebe.msgpack.spec.MsgPackWriter;
import io.zeebe.protocol.Protocol;
import java.util.List;
import java.util.stream.Collectors;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;

public final class UserTaskTransformer implements ModelElementTransformer<UserTask> {

  private static final Logger LOG = Loggers.STREAM_PROCESSING;

  private static final int INITIAL_SIZE_KEY_VALUE_PAIR = 128;

  private final MsgPackWriter msgPackWriter = new MsgPackWriter();

  @Override
  public Class<UserTask> getType() {
    return UserTask.class;
  }

  @Override
  public void transform(final UserTask element, final TransformContext context) {

    final ExecutableProcess process = context.getCurrentProcess();
    final ExecutableUserTask userTask =
        process.getElementById(element.getId(), ExecutableUserTask.class);

    transformTaskDefinition(element, userTask, context);

    transformTaskHeaders(element, userTask);
  }

  private void transformTaskDefinition(
      final UserTask element, final ExecutableUserTask userTask, final TransformContext context) {
    final ZeebeTaskDefinition taskDefinition =
        element.getSingleExtensionElement(ZeebeTaskDefinition.class);

    final ExpressionLanguage expressionLanguage = context.getExpressionLanguage();
    final Expression jobTypeExpression =
        expressionLanguage.parseExpression(Protocol.USER_TASK_JOB_TYPE);

    userTask.setType(jobTypeExpression);
    final Expression retriesExpression = expressionLanguage.parseExpression("1");

    userTask.setRetries(retriesExpression);
  }

  private void transformTaskHeaders(final UserTask element, final ExecutableUserTask userTask) {

    final ZeebeTaskHeaders taskHeaders = getTaskHeaders(element);
    if (taskHeaders != null) {
      final List<ZeebeHeader> validHeaders =
          taskHeaders.getHeaders().stream()
              .filter(this::isValidHeader)
              .collect(Collectors.toList());

      if (validHeaders.size() < taskHeaders.getHeaders().size()) {
        LOG.warn(
            "Ignoring invalid headers for task '{}'. Must have non-empty key and value.",
            element.getName());
      }

      if (!validHeaders.isEmpty()) {
        final DirectBuffer encodedHeaders = encode(validHeaders);
        userTask.setEncodedHeaders(encodedHeaders);
      }
    }
  }

  public ZeebeTaskHeaders getTaskHeaders(UserTask element) {
    ZeebeTaskHeaders taskHeaders = element.getSingleExtensionElement(ZeebeTaskHeaders.class);

    final ZeebeFormDefinition zeebeFormDefinition =
        element.getSingleExtensionElement(ZeebeFormDefinition.class);

    if (zeebeFormDefinition != null) {
      final String formKey = zeebeFormDefinition.getFormKey();
      if (formKey != null) {
        final ZeebeHeader formKeyHeader = element.getModelInstance().newInstance(ZeebeHeader.class);
        formKeyHeader.setKey(Protocol.USER_TASK_FORM_KEY_HEADER_NAME);
        formKeyHeader.setValue(formKey);

        if (taskHeaders == null) {
          taskHeaders = element.getModelInstance().newInstance(ZeebeTaskHeaders.class);
        }

        taskHeaders.getHeaders().add(formKeyHeader);
      }
    }

    return taskHeaders;
  }

  private DirectBuffer encode(final List<ZeebeHeader> taskHeaders) {
    final MutableDirectBuffer buffer = new UnsafeBuffer(0, 0);

    final ExpandableArrayBuffer expandableBuffer =
        new ExpandableArrayBuffer(INITIAL_SIZE_KEY_VALUE_PAIR * taskHeaders.size());

    msgPackWriter.wrap(expandableBuffer, 0);
    msgPackWriter.writeMapHeader(taskHeaders.size());

    taskHeaders.forEach(
        h -> {
          if (isValidHeader(h)) {
            final DirectBuffer key = wrapString(h.getKey());
            msgPackWriter.writeString(key);

            final DirectBuffer value = wrapString(h.getValue());
            msgPackWriter.writeString(value);
          }
        });

    buffer.wrap(expandableBuffer.byteArray(), 0, msgPackWriter.getOffset());

    return buffer;
  }

  private boolean isValidHeader(final ZeebeHeader header) {
    return header != null
        && header.getValue() != null
        && !header.getValue().isEmpty()
        && header.getKey() != null
        && !header.getKey().isEmpty();
  }
}
