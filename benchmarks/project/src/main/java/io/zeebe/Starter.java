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

    final ScheduledExecutorService executorService =
        Executors.newScheduledThreadPool(starterCfg.getThreads());

    // start instances
    LOG.info("Creating an deployment every second");

    final CountDownLatch countDownLatch = new CountDownLatch(1);

    final ScheduledFuture scheduledTask =
        executorService.scheduleAtFixedRate(
            () -> {
              // continuous deployment yeah
              LOG.info("Creating an deployment");
              final var modelInstance =
                  Bpmn.createExecutableProcess()
                      .startEvent()
                      .timerWithDateExpression("now() + duration(\"PT1S\")")
                      .serviceTask("task", t -> t.zeebeJobType("task"))
                      .boundaryEvent()
                      .timerWithDuration("PT1M")
                      .connectTo("task")
                      .endEvent()
                      .done();

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

  private ZeebeClient createZeebeClient() {
    final ZeebeClientBuilder builder =
        ZeebeClient.newClientBuilder()
            .credentialsProvider(
                new OAuthCredentialsProviderBuilder()
                    .clientId("2wYwI6sHllQddtIPbe3aRHkmfh5vu9.B")
                    .clientSecret(
                        "wZxle8Fs~97Q~5p3lrY3rzRUIvJzA2IPlVaceRJDkHebkQ.5nhLsuZph~BKW3BOm")
                    .audience("391dbb09-d55f-44a5-b3a5-44c063762857.zeebe.ultrawombat.com")
                    .authorizationServerUrl("https://login.cloud.ultrawombat.com/oauth/token")
                    .build())
            .gatewayAddress("391dbb09-d55f-44a5-b3a5-44c063762857.zeebe.ultrawombat.com:443")
            .numJobWorkerExecutionThreads(0)
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
