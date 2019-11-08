/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.configuration;

public class BackpressureCfg implements ConfigurationEntry {

  private boolean enabled = true;
  private boolean useWindowed = true;
  private String algorithm = "vegas";
  private long minWindowTime = 500; // milli seconds
  private long maxWindowTime = 2000; // milli seconds
  private long minRttThreshold = 100; // milli seconds

  public long getMinRttThreshold() {
    return minRttThreshold;
  }

  public BackpressureCfg setMinRttThreshold(long minRttThreshold) {
    this.minRttThreshold = minRttThreshold;
    return this;
  }

  public long getMinWindowTime() {
    return minWindowTime;
  }

  public BackpressureCfg setMinWindowTime(long minWindowTime) {
    this.minWindowTime = minWindowTime;
    return this;
  }

  public long getMaxWindowTime() {
    return maxWindowTime;
  }

  public BackpressureCfg setMaxWindowTime(long maxWindowTime) {
    this.maxWindowTime = maxWindowTime;
    return this;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public BackpressureCfg setEnabled(boolean enabled) {
    this.enabled = enabled;
    return this;
  }

  public boolean useWindowed() {
    return useWindowed;
  }

  public BackpressureCfg setUseWindowed(boolean useWindowed) {
    this.useWindowed = useWindowed;
    return this;
  }

  public LimitAlgorithm getAlgorithm() {
    return LimitAlgorithm.valueOf(algorithm.toUpperCase());
  }

  public BackpressureCfg setAlgorithm(String algorithm) {
    this.algorithm = algorithm;
    return this;
  }

  public enum LimitAlgorithm {
    VEGAS,
    GRADIENT,
    GRADIENT2
  }
}
