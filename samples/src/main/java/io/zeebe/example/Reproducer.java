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
package io.zeebe.example;

import io.zeebe.client.ZeebeClient;
import io.zeebe.client.api.ZeebeFuture;
import io.zeebe.client.api.events.WorkflowInstanceEvent;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.util.collection.Tuple;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reproducer {

  public static final Logger LOG = LoggerFactory.getLogger(Reproducer.class);

  public static void main(String[] args) {
    final ZeebeClient client = ZeebeClient.newClient();
    final long workflowKey =
        client
            .newDeployCommand()
            .addWorkflowModel(
                Bpmn.createExecutableProcess().startEvent().endEvent().done(), "process.bpmn")
            .send()
            .join()
            .getWorkflows()
            .get(0)
            .getWorkflowKey();

    final BlockingQueue<Tuple<Integer, ZeebeFuture<WorkflowInstanceEvent>>> futures =
        new ArrayBlockingQueue<>(10_000);
    final int requests = 100_000;
    final AtomicBoolean finished = new AtomicBoolean(false);

    final Thread responseChecker =
        new Thread(
            () -> {
              while (!(futures.isEmpty() && finished.get())) {
                final Tuple<Integer, ZeebeFuture<WorkflowInstanceEvent>> responseTuple;
                try {
                  responseTuple = futures.take();
                } catch (InterruptedException e) {
                  continue;
                }

                try {
                  responseTuple.getRight().get();
                } catch (InterruptedException e) {
                  LOG.warn("Interrupted while waiting for response {}", responseTuple.getLeft(), e);
                } catch (ExecutionException e) {
                  LOG.error("Failed to receive response {}", responseTuple.getLeft());
                }
              }
            });

    responseChecker.start();

    for (int i = 1; i <= requests; i++) {
      try {
        futures.put(
            new Tuple<>(
                i,
                client
                    .newCreateInstanceCommand()
                    .workflowKey(workflowKey)
                    .variables(Collections.singletonMap("FINDME", "INSTANCE-" + i))
                    .send()));
      } catch (InterruptedException e) {
        LOG.warn("Failed to add request {} to queue", i, e);
      }
    }

    finished.set(true);

    try {
      responseChecker.join();
    } catch (InterruptedException e) {
      LOG.warn("Failed to wait for response checker to finish");
    }
  }
}
