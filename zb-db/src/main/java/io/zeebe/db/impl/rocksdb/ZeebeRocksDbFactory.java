/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.db.impl.rocksdb;

import io.zeebe.db.DbKey;
import io.zeebe.db.ZeebeDbFactory;
import io.zeebe.db.impl.DbShort;
import io.zeebe.db.impl.rocksdb.transaction.ZeebeTransactionDb;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.agrona.CloseHelper;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionPriority;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;

public final class ZeebeRocksDbFactory<ColumnFamilyType extends Enum<ColumnFamilyType>>
    implements ZeebeDbFactory<ColumnFamilyType> {

  static {
    RocksDB.loadLibrary();
  }

  private final Class<ColumnFamilyType> columnFamilyTypeClass;
  private final Properties userProvidedColumnFamilyOptions;

  private ZeebeRocksDbFactory(
      final Class<ColumnFamilyType> columnFamilyTypeClass,
      final Properties userProvidedColumnFamilyOptions) {
    this.columnFamilyTypeClass = columnFamilyTypeClass;
    this.userProvidedColumnFamilyOptions = Objects.requireNonNull(userProvidedColumnFamilyOptions);
  }

  public static <ColumnFamilyType extends Enum<ColumnFamilyType>>
      ZeebeDbFactory<ColumnFamilyType> newFactory(
          final Class<ColumnFamilyType> columnFamilyTypeClass) {
    final var columnFamilyOptions = new Properties();
    return new ZeebeRocksDbFactory<>(columnFamilyTypeClass, columnFamilyOptions);
  }

  public static <ColumnFamilyType extends Enum<ColumnFamilyType>>
      ZeebeDbFactory<ColumnFamilyType> newFactory(
          final Class<ColumnFamilyType> columnFamilyTypeClass,
          final Properties userProvidedColumnFamilyOptions) {
    return new ZeebeRocksDbFactory<>(columnFamilyTypeClass, userProvidedColumnFamilyOptions);
  }

  @Override
  public ZeebeTransactionDb<ColumnFamilyType> createDb(final File pathName) {
    return open(pathName, columnFamilyTypeClass.getEnumConstants());
  }

  private ZeebeTransactionDb<ColumnFamilyType> open(
      final File dbDirectory, final ColumnFamilyType[] columnFamilies) {
    final List<AutoCloseable> closeables = new ArrayList<>();

    try {
      // column family options have to be closed as last
      final ColumnFamilyOptions columnFamilyOptions = createColumnFamilyOptions();
      closeables.add(columnFamilyOptions);

      final Statistics statistics = new Statistics();
      closeables.add(statistics);
      statistics.setStatsLevel(StatsLevel.ALL);

      final DBOptions dbOptions =
          new DBOptions()
              .setCreateMissingColumnFamilies(true)
              .setErrorIfExists(false)
              .setCreateIfMissing(true)
              .setParanoidChecks(true)
              .setAvoidFlushDuringRecovery(true)
              .setMaxManifestFileSize(256 * 1024 * 1024L)
              .setStatsDumpPeriodSec(10)
              .setStatistics(statistics)
              .setAdviseRandomOnOpen(false)
              .setBytesPerSync(1048576L);
      closeables.add(dbOptions);

      final Options options = new Options(dbOptions, columnFamilyOptions);
      closeables.add(options);

      // TODO: enforce the key type here to be the same as the instance we pass
      final DbKey prefixKeyInstance = new DbShort();
      final Map<ColumnFamilyType, DbKey> columnPrefixMap = computeColumnPrefixMap(columnFamilies);

      return ZeebeTransactionDb.openTransactionalDb(
          options, dbDirectory.getAbsolutePath(), columnPrefixMap, prefixKeyInstance, closeables);
    } catch (final RocksDBException e) {
      CloseHelper.quietCloseAll(closeables);
      throw new RuntimeException("Unexpected error occurred trying to open the database", e);
    }
  }

  private EnumMap<ColumnFamilyType, DbKey> computeColumnPrefixMap(
      final ColumnFamilyType[] columnFamilies) {
    return Arrays.stream(columnFamilies)
        .reduce(
            new EnumMap<>(columnFamilyTypeClass),
            (acc, cfType) -> {
              acc.put(cfType, new DbShort((short) cfType.ordinal()));
              return acc;
            },
            (left, right) -> {
              left.putAll(right);
              return left;
            });
  }

  /** @return Options which are used on all column families */
  public ColumnFamilyOptions createColumnFamilyOptions() {
    // start with some defaults
    final var columnFamilyOptionProps = new Properties();
    // look for cf_options.h to find available keys
    // look for options_helper.cc to find available values
    columnFamilyOptionProps.put("compaction_pri", "kOldestSmallestSeqFirst");

    // apply custom options
    columnFamilyOptionProps.putAll(userProvidedColumnFamilyOptions);

    final var columnFamilyOptions =
        ColumnFamilyOptions.getColumnFamilyOptionsFromProps(columnFamilyOptionProps);
    if (columnFamilyOptions == null) {
      throw new IllegalStateException(
          String.format(
              "Expected to create column family options for RocksDB, "
                  + "but one or many values are undefined in the context of RocksDB "
                  + "[Compiled ColumnFamilyOptions: %s; User-provided ColumnFamilyOptions: %s]. "
                  + "See RocksDB's cf_options.h and options_helper.cc for available keys and values.",
              columnFamilyOptionProps, userProvidedColumnFamilyOptions));
    }

    return columnFamilyOptions
        .setMinWriteBufferNumberToMerge(2)
        .setMaxWriteBufferNumberToMaintain(12)
        .setMaxWriteBufferNumber(12)
        .setWriteBufferSize(32 * 1024 * 1024L)
        .setLevelCompactionDynamicLevelBytes(true)
        .setCompactionPriority(CompactionPriority.MinOverlappingRatio)
        .setCompressionType(CompressionType.NO_COMPRESSION)
        .setCompactionStyle(CompactionStyle.LEVEL)
        .setTargetFileSizeBase(8 * 1024 * 1024L)
        .setLevel0FileNumCompactionTrigger(8)
        .setLevel0SlowdownWritesTrigger(17)
        .setLevel0StopWritesTrigger(24)
        .setNumLevels(4)
        .setMaxBytesForLevelBase(32 * 1024 * 1024L)
        .setMemtablePrefixBloomSizeRatio(0.25)
        .useFixedLengthPrefixExtractor(Short.BYTES)
        .setTableFormatConfig(
            new BlockBasedTableConfig()
                .setBlockCache(new LRUCache(178956970L))
                .setWholeKeyFiltering(false)
                .setFilterPolicy(new BloomFilter(10, false))
                .setBlockSize(16 * 1024L)
                .setCacheIndexAndFilterBlocks(true)
                .setPinL0FilterAndIndexBlocksInCache(true));
  }
}
