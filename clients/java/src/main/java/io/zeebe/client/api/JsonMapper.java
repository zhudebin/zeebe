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
package io.zeebe.client.api;

import java.io.InputStream;
import java.util.Map;

/**
 * This interface is using to customize the way how objects will be transformed in JSON format.
 *
 * @see io.zeebe.client.impl.ZeebeObjectMapper
 */
public interface JsonMapper {

  /**
   * Transform a JSON string to the typed object.
   *
   * @param json a JSON string
   * @param typeClass a class of the type to serialize
   * @param <T> a type of the returned object
   * @return a typed object from a JSON string
   */
  <T> T fromJson(final String json, final Class<T> typeClass);

  /**
   * Transform a JSON string to the map with string to object pairs
   *
   * @param json a JSON string
   * @return a map is filled with JSON
   */
  Map<String, Object> fromJsonAsMap(final String json);

  /**
   * Transform a JSON string to the map with string to string pairs
   *
   * @param json a JSON string
   * @return a map is filled with JSON
   */
  Map<String, String> fromJsonAsStringMap(final String json);

  /**
   * Transform an object to a JSON string
   *
   * @param value an object that will be transformed
   * @return a JSON string
   */
  String toJson(final Object value);

  /**
   * Validate that a jsonInput is actually a JSON string. If not throws a {@link
   * io.zeebe.client.api.command.InternalClientException}
   *
   * @param propertyName a property name that contains jsonInput
   * @param jsonInput a JSON string
   * @return a JSON string
   * @throws io.zeebe.client.api.command.InternalClientException when a jsonInput is not a JSON
   *     string
   */
  String validateJson(final String propertyName, final String jsonInput);

  /**
   * Validate that a jsonInput is actually a JSON string. If not throws a {@link
   * io.zeebe.client.api.command.InternalClientException}
   *
   * @param propertyName a property name that contains jsonInput
   * @param jsonInput a stream that contains a JSON string
   * @return a JSON string
   * @throws io.zeebe.client.api.command.InternalClientException when a jsonInput doesn't contains a
   *     JSON string
   */
  String validateJson(final String propertyName, final InputStream jsonInput);
}
