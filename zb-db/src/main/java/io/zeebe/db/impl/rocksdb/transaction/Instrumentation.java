/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.db.impl.rocksdb.transaction;

import io.prometheus.client.Histogram;

final class Instrumentation {
  // RocksDB methods tend to be very fast, in the order of micro seconds or < 5ms, therefore the
  // default buckets are too big to see anything
  private static final double[] BUCKETS =
      new double[] {0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2};
  private static final String NAMESPACE = "zeebe";

  static final Histogram PUT =
      Histogram.build()
          .namespace(NAMESPACE)
          .name("rocksdb_perf_put")
          .help("Performance of ColumnFamily#put")
          .buckets(BUCKETS)
          .register();
  static final Histogram GET =
      Histogram.build()
          .namespace(NAMESPACE)
          .name("rocksdb_perf_get")
          .help("Performance of ColumnFamily#get")
          .buckets(BUCKETS)
          .register();
  static final Histogram FOREACH =
      Histogram.build()
          .namespace(NAMESPACE)
          .name("rocksdb_perf_foreach")
          .help("Performance of ColumnFamily#foreach")
          .buckets(BUCKETS)
          .register();
  static final Histogram WHILETRUE =
      Histogram.build()
          .namespace(NAMESPACE)
          .name("rocksdb_perf_whileTrue")
          .help("Performance of ColumnFamily#whileTrue")
          .buckets(BUCKETS)
          .register();
  static final Histogram WHILEEQUALPREFIX =
      Histogram.build()
          .namespace(NAMESPACE)
          .name("rocksdb_perf_whileEqualPrefix")
          .help("Performance of ColumnFamily#whileEqualPrefix")
          .buckets(BUCKETS)
          .register();
  static final Histogram DELETE =
      Histogram.build()
          .namespace(NAMESPACE)
          .name("rocksdb_perf_delete")
          .help("Performance of ColumnFamily#delete")
          .buckets(BUCKETS)
          .register();
  static final Histogram EXISTS =
      Histogram.build()
          .namespace(NAMESPACE)
          .name("rocksdb_perf_exists")
          .help("Performance of ColumnFamily#exists")
          .buckets(BUCKETS)
          .register();
  static final Histogram ISEMPTY =
      Histogram.build()
          .namespace(NAMESPACE)
          .name("rocksdb_perf_isEmpty")
          .help("Performance of ColumnFamily#isEmpty")
          .buckets(BUCKETS)
          .register();
  static final Histogram COMMIT =
      Histogram.build()
          .namespace(NAMESPACE)
          .name("rocksdb_perf_commit")
          .help("Performance of ZeebeDbTransaction#commit")
          .buckets(BUCKETS)
          .register();
  static final Histogram ROLLBACK =
      Histogram.build()
          .namespace(NAMESPACE)
          .name("rocksdb_perf_rollback")
          .help("Performance of ZeebeDbTransaction#rollback")
          .buckets(BUCKETS)
          .register();
  static final Histogram SEEK =
      Histogram.build()
          .namespace(NAMESPACE)
          .name("rocksdb_perf_seek")
          .help("Performance of RocksIterator#seek")
          .buckets(BUCKETS)
          .register();

  private Instrumentation() {}
}
