/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.e2e.util.containers.hazelcast;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

public final class HazelcastContainer extends GenericContainer<HazelcastContainer> {
  private static final String DEFAULT_IMAGE = "hazelcast/hazelcast";
  private static final String DEFAULT_VERSION = "3.12.6";
  private static final int HAZELCAST_PORT = 5701;

  private final Map<String, String> javaOpts;

  public HazelcastContainer() {
    this(DEFAULT_IMAGE + ":" + DEFAULT_VERSION);
  }

  public HazelcastContainer(final String dockerImageName) {
    super(dockerImageName);
    this.javaOpts = new HashMap<>();

    applyDefaultJavaOptions();
    setWaitStrategy(
        new HttpWaitStrategy()
            .withReadTimeout(Duration.ofSeconds(30))
            .forStatusCodeMatching(code -> code >= 200 && code < 400)
            .forPort(HAZELCAST_PORT)
            .forPath("/hazelcast/health/node-state"));
    addExposedPorts(HAZELCAST_PORT);
  }

  public HazelcastContainer withJavaOption(final String option, final String value) {
    javaOpts.put(option, value);
    return updateJavaOpts();
  }

  public String getInternalAddress() {
    final var aliases = getNetworkAliases();
    final var host = aliases.isEmpty() ? getContainerIpAddress() : aliases.get(0);
    return getAddress(host, HAZELCAST_PORT);
  }

  public String getExternalAddress() {
    return getAddress("localhost", getMappedPort(HAZELCAST_PORT));
  }

  @Override
  public boolean equals(final Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    if (!super.equals(other)) {
      return false;
    }

    final HazelcastContainer that = (HazelcastContainer) other;
    return javaOpts.equals(that.javaOpts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), javaOpts);
  }

  public HttpWaitStrategy getDefaultWaitStrategy() {
    return new HttpWaitStrategy()
        .withReadTimeout(Duration.ofSeconds(30))
        .forStatusCodeMatching(code -> code >= 200 && code < 400)
        .forPort(HAZELCAST_PORT)
        .forPath("/hazelcast/health/node-state");
  }

  private String getAddress(final String host, final int port) {
    return String.format("%s:%d", host, port);
  }

  /**
   * Applies sane default system properties.
   *
   * <ul>
   *   <li>Enables the REST API to enable the health check endpoint
   *   <li>Forces Hazelcast to bind to the any interface to accept all incoming connections
   *   <li>Disable telemetry pings to Hazelcast
   * </ul>
   */
  private void applyDefaultJavaOptions() {
    withJavaOption("hazelcast.rest.enabled", "true")
        .withJavaOption("hazelcast.socket.bind.any", "true")
        .withJavaOption("hazelcast.phone.home.enabled", "false");
  }

  private HazelcastContainer updateJavaOpts() {
    final var options = new ArrayList<String>(javaOpts.size());
    for (final var option : javaOpts.entrySet()) {
      options.add(String.format("-D%s=%s", option.getKey(), option.getValue()));
    }

    return withEnv("JAVA_OPTS", String.join(" ", options));
  }
}
