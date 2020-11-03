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
package io.zeebe.client.impl.command;

import io.zeebe.client.impl.ZeebeObjectMapper;
import io.zeebe.client.impl.ZeebeObjectMapperWrapper;
import java.io.InputStream;
import java.util.Map;

public abstract class CommandWithVariables<T> {

  protected final ZeebeObjectMapper objectMapper;
  protected final ZeebeObjectMapperWrapper zeebeObjectMapperWrapper;

  /**
   * This constructor is deprecated. Saved for backward compatibility.
   *
   * @see #CommandWithVariables(ZeebeObjectMapperWrapper)
   * @deprecated
   */
  @Deprecated
  public CommandWithVariables(final ZeebeObjectMapper objectMapper) {
    this(new ZeebeObjectMapperWrapper(objectMapper));
  }

  public CommandWithVariables(final ZeebeObjectMapperWrapper zeebeObjectMapperWrapper) {
    this.zeebeObjectMapperWrapper = zeebeObjectMapperWrapper;
    this.objectMapper = new ZeebeObjectMapper();
  }

  public T variables(final InputStream variables) {
    ArgumentUtil.ensureNotNull("variables", variables);
    return setVariablesInternal(zeebeObjectMapperWrapper.validateJson("variables", variables));
  }

  public T variables(final String variables) {
    ArgumentUtil.ensureNotNull("variables", variables);
    return setVariablesInternal(zeebeObjectMapperWrapper.validateJson("variables", variables));
  }

  public T variables(final Map<String, Object> variables) {
    ArgumentUtil.ensureNotNull("variables", variables);
    return variables((Object) variables);
  }

  public T variables(final Object variables) {
    ArgumentUtil.ensureNotNull("variables", variables);
    return setVariablesInternal(zeebeObjectMapperWrapper.toJson(variables));
  }

  protected abstract T setVariablesInternal(String variables);
}
