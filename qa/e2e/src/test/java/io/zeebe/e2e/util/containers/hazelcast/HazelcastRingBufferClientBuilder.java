/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.e2e.util.containers.hazelcast;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import io.zeebe.e2e.util.containers.ExporterClientListener;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public final class HazelcastRingBufferClientBuilder {
  private final Set<ExporterClientListener> listeners;
  private String hazelcastAddress;
  private String ringBufferName;

  public HazelcastRingBufferClientBuilder() {
    this.listeners = new HashSet<>();
  }

  public HazelcastRingBufferClientBuilder withHazelcastAddress(final String hazelcastAddress) {
    this.hazelcastAddress = hazelcastAddress;
    return this;
  }

  public HazelcastRingBufferClientBuilder withListener(final ExporterClientListener... listeners) {
    this.listeners.addAll(Arrays.asList(listeners));
    return this;
  }

  public HazelcastRingBufferClientBuilder withRingBufferName(final String name) {
    this.ringBufferName = name;
    return this;
  }

  public HazelcastRingBufferClient build() {
    validate();

    final var hazelcast = HazelcastClient.newHazelcastClient(getClientConfig());
    final var ringBuffer = hazelcast.<byte[]>getRingbuffer(ringBufferName);
    return new HazelcastRingBufferClient(
        hazelcast, ringBuffer, ringBuffer.headSequence(), listeners);
  }

  private ClientConfig getClientConfig() {
    final var config = new ClientConfig();
    config.setProperty("hazelcast.logging.type", "log4j2");
    config.getNetworkConfig().addAddress(hazelcastAddress);
    return config;
  }

  private void validate() {
    if (ringBufferName == null || ringBufferName.isEmpty()) {
      throw new IllegalArgumentException(
          "Expected ring buffer name to be something, but nothing given");
    }

    if (hazelcastAddress == null || hazelcastAddress.isEmpty()) {
      throw new IllegalArgumentException(
          "Expected Hazelcast address to be something, but none given");
    }
  }
}
