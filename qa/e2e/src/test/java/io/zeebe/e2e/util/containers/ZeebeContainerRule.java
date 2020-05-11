/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.e2e.util.containers;

import io.zeebe.client.ZeebeClient;
import io.zeebe.containers.ZeebeBrokerContainer;
import io.zeebe.containers.ZeebePort;
import io.zeebe.e2e.util.containers.debug.DebugHttpExporterClient;
import io.zeebe.e2e.util.containers.hazelcast.HazelcastContainer;
import io.zeebe.e2e.util.containers.hazelcast.HazelcastRingBufferClient;
import io.zeebe.e2e.util.record.RecordRepository;
import org.junit.rules.ExternalResource;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.MountableFile;

public final class ZeebeContainerRule extends ExternalResource {

  private static final String HAZELCAST_RING_BUFFER_NAME = "zeebe";
  private static final String ZEEBE_CONTAINER_ALIAS = "zeebe";
  private static final String HAZELCAST_CONTAINER_ALIAS = "hazelcast";
  private static final int DEBUG_HTTP_EXPORTER_PORT = 8000;
  private static final String ZEEBE_VERSION = "current-test"; // "0.23.1";

  private final Network network = Network.newNetwork();
  private final ZeebeBrokerContainer zeebe = defaultZeebeContainer();
  private final HazelcastContainer hazelcast = defaultHazelcastContainer();

  private HazelcastRingBufferClient ringBufferClient;
  private DebugHttpExporterClient httpExporterClient;

  @Override
  protected void before() {
    // hazelcast.start();
    zeebe.start();

    httpExporterClient = newHttpExporterClient();
    httpExporterClient.start();
    // ringBufferClient = newRingBufferClient();
    // ringBufferClient.start();
  }

  @Override
  protected void after() {
    //    if (ringBufferClient != null) {
    //      ringBufferClient.stop();
    //    }
    if (httpExporterClient != null) {
      httpExporterClient.stop();
    }

    zeebe.stop();
    // hazelcast.stop();
  }

  public ZeebeClient newZeebeClient() {
    return ZeebeClient.newClientBuilder()
        .brokerContactPoint(zeebe.getExternalAddress(ZeebePort.GATEWAY))
        .usePlaintext()
        .build();
  }

  public RecordRepository newRecordRepository() {
    final var repository = new RecordRepository();
    final var listener = new RecordRepositoryExporterClientListener(repository);

    if (ringBufferClient != null) {
      ringBufferClient.addListener(listener);
    }

    if (httpExporterClient != null) {
      httpExporterClient.addListener(listener);
    }

    return repository;
  }

  public HazelcastRingBufferClient newRingBufferClient() {
    return HazelcastRingBufferClient.builder()
        .withRingBufferName(HAZELCAST_RING_BUFFER_NAME)
        .withHazelcastAddress(hazelcast.getExternalAddress())
        .build();
  }

  public DebugHttpExporterClient newHttpExporterClient() {
    return DebugHttpExporterClient.builder()
        .withPort(zeebe.getMappedPort(DEBUG_HTTP_EXPORTER_PORT))
        .build();
  }

  private ZeebeBrokerContainer defaultZeebeContainer() {
    final var zeebeContainer =
        new ZeebeBrokerContainer(ZEEBE_VERSION)
            .withNetwork(network)
            .withNetworkAliases(ZEEBE_CONTAINER_ALIAS)
            .withDebug(false);
    // return configureHazelcastExporter(zeebeContainer);
    return configureDebugHttpExporter(zeebeContainer);
  }

  private HazelcastContainer defaultHazelcastContainer() {
    return new HazelcastContainer()
        .withNetwork(network)
        .withNetworkAliases(HAZELCAST_CONTAINER_ALIAS);
  }

  private ZeebeBrokerContainer configureDebugHttpExporter(
      final ZeebeBrokerContainer zeebeContainer) {
    zeebeContainer.addExposedPorts(DEBUG_HTTP_EXPORTER_PORT);
    return zeebeContainer
        .withEnv(
            "ZEEBE_BROKER_EXPORTERS_DEBUGHTTP_CLASSNAME",
            "io.zeebe.broker.exporter.debug.DebugHttpExporter")
        .withEnv("ZEEBE_BROKER_EXPORTERS_DEBUGHTTP_ARGS_HOST", "0.0.0.0")
        .withEnv(
            "ZEEBE_BROKER_EXPORTERS_DEBUGHTTP_ARGS_PORT", String.valueOf(DEBUG_HTTP_EXPORTER_PORT));
  }

  private ZeebeBrokerContainer configureHazelcastExporter(
      final ZeebeBrokerContainer zeebeContainer) {
    return zeebeContainer
        .withCopyFileToContainer(
            MountableFile.forClasspathResource("zeebe-hazelcast-exporter.jar"),
            "/usr/local/zeebe/exporters/hazelcast.jar")
        .withEnv(
            "ZEEBE_BROKER_EXPORTERS_HAZELCAST_CLASSNAME",
            "io.zeebe.hazelcast.exporter.HazelcastExporter")
        .withEnv(
            "ZEEBE_BROKER_EXPORTERS_HAZELCAST_JARPATH", "/usr/local/zeebe/exporters/hazelcast.jar")
        .withEnv("ZEEBE_HAZELCAST_REMOTE_ADDRESS", "hazelcast:5701")
        .withEnv("ZEEBE_BROKER_EXPORTERS_HAZELCAST_ARGS_REMOTEADDRESS", "hazelcast:5701")
        .withEnv("ZEEBE_BROKER_EXPORTERS_HAZELCAST_ARGS_NAME", HAZELCAST_RING_BUFFER_NAME)
        .withEnv("ZEEBE_BROKER_EXPORTERS_HAZELCAST_ARGS_FORMAT", "json");
  }
}
