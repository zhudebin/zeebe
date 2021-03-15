/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
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
package io.zeebe;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import io.zeebe.client.ZeebeClient;
import io.zeebe.client.ZeebeClientBuilder;
import io.zeebe.client.impl.oauth.OAuthCredentialsProviderBuilder;
import io.zeebe.config.AppCfg;
import io.zeebe.config.StarterCfg;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Starter extends App {

  private static final Logger LOG = LoggerFactory.getLogger(Starter.class);

  private final AppCfg appCfg;

  Starter(final AppCfg appCfg) {
    this.appCfg = appCfg;
  }

  @Override
  public void run() {
    final StarterCfg starterCfg = appCfg.getStarter();
    final ZeebeClient client = createZeebeClient();

    printTopology(client);

    client
        .newWorker()
        .jobType("build")
        .handler(
            (client1, job) -> {
              Thread.sleep(15 * 1000);
              client1.newCompleteCommand(job.getKey()).variables(Map.of("result", true)).send();
            })
        .open();

    client
        .newWorker()
        .jobType("report")
        .handler((client1, job) -> client1.newCompleteCommand(job.getKey()).send())
        .open();

    final ScheduledExecutorService executorService =
        Executors.newScheduledThreadPool(starterCfg.getThreads());

    // start instances
    LOG.info("Creating an deployment every second");

    final CountDownLatch countDownLatch = new CountDownLatch(1);

    final ScheduledFuture scheduledTask =
        executorService.scheduleAtFixedRate(
            () -> {
              // continuous deployment yeah
              final var modelInstance = createModel();

              //              LOG.info("Creating an deployment {}",
              // Bpmn.convertToString(modelInstance));
              while (true) {
                try {
                  client.newDeployCommand().addWorkflowModel(modelInstance, "timer").send().join();
                  LOG.info("Deployed");
                  break;
                } catch (final Exception e) {
                  LOG.warn("Failed to deploy workflow, retrying", e);
                  try {
                    Thread.sleep(200);
                  } catch (final InterruptedException ex) {
                    // ignore
                  }
                }
              }
            },
            0,
            1,
            TimeUnit.SECONDS);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  if (!executorService.isShutdown()) {
                    executorService.shutdown();
                    try {
                      executorService.awaitTermination(60, TimeUnit.SECONDS);
                    } catch (final InterruptedException e) {
                      LOG.error("Shutdown executor service was interrupted", e);
                    }
                  }
                }));

    // wait for starter to finish
    try {
      countDownLatch.await();
    } catch (final InterruptedException e) {
      LOG.error("Awaiting of count down latch was interrupted.", e);
    }

    LOG.info("Starter finished");

    scheduledTask.cancel(true);
    executorService.shutdown();
  }

  private BpmnModelInstance createModel() {
    final var executableProcess = Bpmn.createExecutableProcess("buildProcess");

    executableProcess
        .eventSubProcess()
        .startEvent()
        .timerWithDuration("PT15S")
        .interrupting(false)
        .serviceTask("report", task -> task.zeebeJobType("report"))
        .endEvent();

    return executableProcess
        .startEvent()
        .timerWithDateExpression("now() + duration(\"PT1S\")")
        .serviceTask("build", t -> t.zeebeJobType("build"))
        .exclusiveGateway("xor")
        .sequenceFlowId("successFlow")
        .condition("=contains(result, \"success\") != null and contains(result, \"success\")")
        .endEvent()
        .moveToLastExclusiveGateway()
        .sequenceFlowId("fail")
        .defaultFlow()
        .intermediateCatchEvent()
        .timerWithDurationExpression("=today() + duration(\"PT15S\")")
        .connectTo("build")
        .done();
  }

  private ZeebeClient createZeebeClient() {

    final ZeebeClientBuilder builder =
        ZeebeClient.newClientBuilder()
            .credentialsProvider(
                new OAuthCredentialsProviderBuilder()
                    .clientId("cRXZMMYP7yjf_08hmx3LQGuy-Zd3WDoC")
                    .clientSecret(
                        "VE3Gkl_kmRGgUICVuWtsobHvhMHLcIce-fKUAWsAH.kDC3QDezaCbB01WxfILO0E")
                    .audience("5e7c668d-8d7f-4faf-886b-36e996ce1766.zeebe.ultrawombat.com")
                    .authorizationServerUrl("https://login.cloud.ultrawombat.com/oauth/token")
                    .build())
            .gatewayAddress("5e7c668d-8d7f-4faf-886b-36e996ce1766.zeebe.ultrawombat.com:443")
            //            .numJobWorkerExecutionThreads(0)
            .withProperties(System.getProperties());
    //            .withInterceptors(monitoringInterceptor);

    //    if (!appCfg.isTls()) {
    //      builder.usePlaintext();
    //    }

    return builder.build();
  }

  public static void main(final String[] args) {
    final Config config = ConfigFactory.load().getConfig("app");
    LOG.info("Starting app with config: {}", config.root().render());
    final AppCfg appCfg = ConfigBeanFactory.create(config, AppCfg.class);
    new Starter(appCfg).run();
  }
}
