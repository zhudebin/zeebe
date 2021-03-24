/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.zeebe.engine.processing.streamprocessor;

import io.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceRecord;
import io.zeebe.protocol.record.ValueType;
import io.zeebe.protocol.record.intent.Intent;
import io.zeebe.protocol.record.value.BpmnElementType;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class MigratedStreamProcessors {

  private static final List<BpmnElementType> MIGRATED_BPMN_PROCESSORS = new ArrayList<>();

  private static final Function<TypedRecord<?>, Boolean> NOT_MIGRATED = record -> false;
  private static final Function<TypedRecord<?>, Boolean> MIGRATED = record -> true;

  private static final Map<ValueType, Function<TypedRecord<?>, Boolean>> MIGRATED_VALUE_TYPES =
      new EnumMap<>(ValueType.class);

  private static final Function<List<Intent>, Function<TypedRecord<?>, Boolean>>
      MIGRATED_INTENT_FILTER_FACTORY =
          (intents) -> (record) -> intents.contains(record.getIntent());

  static {
    MIGRATED_VALUE_TYPES.put(
        ValueType.PROCESS_INSTANCE,
        record -> {
          final var recordValue = (ProcessInstanceRecord) record.getValue();
          final var bpmnElementType = recordValue.getBpmnElementType();
          return MIGRATED_BPMN_PROCESSORS.contains(bpmnElementType);
        });
    MIGRATED_BPMN_PROCESSORS.add(BpmnElementType.TESTING_ONLY);
    MIGRATED_BPMN_PROCESSORS.add(BpmnElementType.EXCLUSIVE_GATEWAY);
    MIGRATED_BPMN_PROCESSORS.add(BpmnElementType.PARALLEL_GATEWAY);
    MIGRATED_BPMN_PROCESSORS.add(BpmnElementType.SUB_PROCESS);

    MIGRATED_VALUE_TYPES.put(ValueType.JOB, MIGRATED);
    MIGRATED_VALUE_TYPES.put(ValueType.JOB_BATCH, MIGRATED);

    MIGRATED_VALUE_TYPES.put(ValueType.ERROR, MIGRATED);
    MIGRATED_VALUE_TYPES.put(ValueType.PROCESS, MIGRATED);
    MIGRATED_VALUE_TYPES.put(ValueType.DEPLOYMENT_DISTRIBUTION, MIGRATED);
    MIGRATED_VALUE_TYPES.put(ValueType.DEPLOYMENT, MIGRATED);
    MIGRATED_VALUE_TYPES.put(ValueType.MESSAGE, MIGRATED);

    MIGRATED_VALUE_TYPES.put(ValueType.MESSAGE_SUBSCRIPTION, MIGRATED);
    MIGRATED_VALUE_TYPES.put(ValueType.MESSAGE_START_EVENT_SUBSCRIPTION, MIGRATED);
    MIGRATED_VALUE_TYPES.put(ValueType.PROCESS_MESSAGE_SUBSCRIPTION, MIGRATED);

    MIGRATED_VALUE_TYPES.put(ValueType.VARIABLE_DOCUMENT, MIGRATED);
    MIGRATED_VALUE_TYPES.put(ValueType.VARIABLE, MIGRATED);
    MIGRATED_VALUE_TYPES.put(ValueType.INCIDENT, MIGRATED);
    MIGRATED_VALUE_TYPES.put(ValueType.TIMER, MIGRATED);
  }

  private MigratedStreamProcessors() {}

  public static boolean isMigrated(final TypedRecord<?> record) {
    final var valueType = record.getValueType();
    return MIGRATED_VALUE_TYPES.getOrDefault(valueType, NOT_MIGRATED).apply(record);
  }

  public static boolean isMigrated(final ValueType valueType) {
    return MIGRATED_VALUE_TYPES.get(valueType) == MIGRATED;
  }

  public static boolean isMigrated(final BpmnElementType bpmnElementType) {
    return MIGRATED_BPMN_PROCESSORS.contains(bpmnElementType);
  }
}
