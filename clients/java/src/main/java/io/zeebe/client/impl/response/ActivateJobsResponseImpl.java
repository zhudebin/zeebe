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
package io.zeebe.client.impl.response;

import io.zeebe.client.api.response.ActivateJobsResponse;
import io.zeebe.client.api.response.ActivatedJob;
import io.zeebe.client.impl.ZeebeObjectMapper;
import io.zeebe.client.impl.ZeebeObjectMapperWrapper;
import java.util.ArrayList;
import java.util.List;

public final class ActivateJobsResponseImpl implements ActivateJobsResponse {

  private final ZeebeObjectMapperWrapper zeebeObjectMapperWrapper;
  private final List<ActivatedJob> jobs = new ArrayList<>();

  /**
   * This constructor is deprecated. Saved for backward compatibility.
   *
   * @see #ActivateJobsResponseImpl(ZeebeObjectMapperWrapper)
   * @deprecated
   */
  @Deprecated
  public ActivateJobsResponseImpl(final ZeebeObjectMapper objectMapper) {
    this(new ZeebeObjectMapperWrapper(objectMapper));
  }

  public ActivateJobsResponseImpl(final ZeebeObjectMapperWrapper zeebeObjectMapperWrapper) {
    this.zeebeObjectMapperWrapper = zeebeObjectMapperWrapper;
  }

  public void addResponse(
      final io.zeebe.gateway.protocol.GatewayOuterClass.ActivateJobsResponse activateJobsResponse) {
    activateJobsResponse.getJobsList().stream()
        .map(r -> new ActivatedJobImpl(zeebeObjectMapperWrapper, r))
        .forEach(jobs::add);
  }

  @Override
  public List<ActivatedJob> getJobs() {
    return jobs;
  }
}
