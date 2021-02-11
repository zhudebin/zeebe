/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.protocol.impl.record.value.deployment;

import static io.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceRecord.PROP_PROCESS_BPMN_PROCESS_ID;
import static io.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceRecord.PROP_PROCESS_KEY;
import static io.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceRecord.PROP_PROCESS_VERSION;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.msgpack.property.IntegerProperty;
import io.zeebe.msgpack.property.LongProperty;
import io.zeebe.msgpack.property.StringProperty;
import io.zeebe.protocol.record.value.deployment.DeployedProcess;
import io.zeebe.util.buffer.BufferUtil;
import org.agrona.DirectBuffer;

public final class Process extends UnpackedObject implements DeployedProcess {
  private final StringProperty bpmnProcessIdProp = new StringProperty(PROP_PROCESS_BPMN_PROCESS_ID);
  private final IntegerProperty versionProp = new IntegerProperty(PROP_PROCESS_VERSION);
  private final LongProperty keyProp = new LongProperty(PROP_PROCESS_KEY);
  private final StringProperty resourceNameProp = new StringProperty("resourceName");

  public Process() {
    declareProperty(bpmnProcessIdProp)
        .declareProperty(versionProp)
        .declareProperty(keyProp)
        .declareProperty(resourceNameProp);
  }

  @Override
  public String getBpmnProcessId() {
    return BufferUtil.bufferAsString(bpmnProcessIdProp.getValue());
  }

  public int getVersion() {
    return versionProp.getValue();
  }

  @Override
  public long getProcessKey() {
    return getKey();
  }

  @Override
  public String getResourceName() {
    return BufferUtil.bufferAsString(resourceNameProp.getValue());
  }

  public Process setResourceName(final String resourceName) {
    resourceNameProp.setValue(resourceName);
    return this;
  }

  public Process setResourceName(final DirectBuffer resourceName) {
    resourceNameProp.setValue(resourceName);
    return this;
  }

  public Process setVersion(final int version) {
    versionProp.setValue(version);
    return this;
  }

  public Process setBpmnProcessId(final String bpmnProcessId) {
    bpmnProcessIdProp.setValue(bpmnProcessId);
    return this;
  }

  public Process setBpmnProcessId(final DirectBuffer bpmnProcessId) {
    bpmnProcessIdProp.setValue(bpmnProcessId);
    return this;
  }

  @JsonIgnore
  public long getKey() {
    return keyProp.getValue();
  }

  public Process setKey(final long key) {
    keyProp.setValue(key);
    return this;
  }

  @JsonIgnore
  public DirectBuffer getBpmnProcessIdBuffer() {
    return bpmnProcessIdProp.getValue();
  }

  @Override
  @JsonIgnore
  public int getEncodedLength() {
    return super.getEncodedLength();
  }

  @Override
  @JsonIgnore
  public int getLength() {
    return super.getLength();
  }

  @JsonIgnore
  public DirectBuffer getResourceNameBuffer() {
    return resourceNameProp.getValue();
  }

  public Process setBpmnProcessId(
      final DirectBuffer bpmnProcessId, final int offset, final int length) {
    bpmnProcessIdProp.setValue(bpmnProcessId, offset, length);
    return this;
  }
}
