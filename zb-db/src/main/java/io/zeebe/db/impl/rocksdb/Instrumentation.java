/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.db.impl.rocksdb;

import io.prometheus.client.Histogram;

final class Instrumentation {
  static final Histogram PUT =
      Histogram.build()
          .namespace("zeebe")
          .name("rocksdb_perf_put")
          .help("Performance of ColumnFamily#put")
          .register();
  static final Histogram GET =
      Histogram.build()
          .namespace("zeebe")
          .name("rocksdb_perf_get")
          .help("Performance of ColumnFamily#get")
          .register();
  static final Histogram FOREACH =
      Histogram.build()
          .namespace("zeebe")
          .name("rocksdb_perf_foreach")
          .help("Performance of ColumnFamily#foreach")
          .register();
  static final Histogram WHILETRUE =
      Histogram.build()
          .namespace("zeebe")
          .name("rocksdb_perf_whileTrue")
          .help("Performance of ColumnFamily#whileTrue")
          .register();
  static final Histogram WHILEEQUALPREFIX =
      Histogram.build()
          .namespace("zeebe")
          .name("rocksdb_perf_whileEqualPrefix")
          .help("Performance of ColumnFamily#whileEqualPrefix")
          .register();
  static final Histogram DELETE =
      Histogram.build()
          .namespace("zeebe")
          .name("rocksdb_perf_delete")
          .help("Performance of ColumnFamily#delete")
          .register();
  static final Histogram EXISTS =
      Histogram.build()
          .namespace("zeebe")
          .name("rocksdb_perf_exists")
          .help("Performance of ColumnFamily#exists")
          .register();
  static final Histogram ISEMPTY =
      Histogram.build()
          .namespace("zeebe")
          .name("rocksdb_perf_isEmpty")
          .help("Performance of ColumnFamily#isEmpty")
          .register();
}
