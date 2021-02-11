/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.gateway.api.deployment;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.zeebe.gateway.api.util.GatewayTest;
import io.zeebe.gateway.impl.broker.request.BrokerDeployProcessRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.DeployProcessRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.DeployProcessRequest.Builder;
import io.zeebe.gateway.protocol.GatewayOuterClass.DeployProcessResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.ProcessMetadata;
import io.zeebe.protocol.record.ValueType;
import io.zeebe.protocol.record.intent.DeploymentIntent;
import org.junit.Test;

public final class DeployProcessTest extends GatewayTest {

  @Test
  public void shouldMapRequestAndResponse() {
    // given
    final DeployProcessStub stub = new DeployProcessStub();
    stub.registerWith(brokerClient);

    final String bpmnName = "testProcess.bpmn";
    final String otherName = "testProcess.txt";

    final Builder builder = DeployProcessRequest.newBuilder();
    builder
        .addProcesssBuilder()
        .setName(bpmnName)
        .setDefinition(ByteString.copyFromUtf8("<xml/>"));
    builder.addProcesssBuilder().setName(otherName).setDefinition(ByteString.copyFromUtf8("test"));

    final DeployProcessRequest request = builder.build();

    // when
    final DeployProcessResponse response = client.deployProcess(request);

    // then
    assertThat(response.getKey()).isEqualTo(stub.getKey());
    assertThat(response.getProcesssCount()).isEqualTo(2);

    ProcessMetadata process = response.getProcesss(0);
    assertThat(process.getBpmnProcessId()).isEqualTo(bpmnName);
    assertThat(process.getResourceName()).isEqualTo(bpmnName);
    assertThat(process.getProcessKey()).isEqualTo(stub.getProcessKey());
    assertThat(process.getVersion()).isEqualTo(stub.getProcessVersion());

    process = response.getProcesss(1);
    assertThat(process.getBpmnProcessId()).isEqualTo(otherName);
    assertThat(process.getResourceName()).isEqualTo(otherName);
    assertThat(process.getProcessKey()).isEqualTo(stub.getProcessKey());
    assertThat(process.getVersion()).isEqualTo(stub.getProcessVersion());

    final BrokerDeployProcessRequest brokerRequest = brokerClient.getSingleBrokerRequest();
    assertThat(brokerRequest.getIntent()).isEqualTo(DeploymentIntent.CREATE);
    assertThat(brokerRequest.getValueType()).isEqualTo(ValueType.DEPLOYMENT);
  }
}
