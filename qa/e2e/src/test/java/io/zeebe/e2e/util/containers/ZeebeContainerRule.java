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

  private final Network network = Network.newNetwork();
  private final ZeebeBrokerContainer zeebe = defaultZeebeContainer();
  private final HazelcastContainer hazelcast = defaultHazelcastContainer();

  private HazelcastRingBufferClient ringBufferClient;

  @Override
  protected void before() {
    hazelcast.start();
    zeebe.start();

    ringBufferClient = newRingBufferClient();
    ringBufferClient.start();
  }

  @Override
  protected void after() {
    if (ringBufferClient != null) {
      ringBufferClient.stop();
    }

    zeebe.stop();
    hazelcast.stop();
  }

  public ZeebeClient newZeebeClient() {
    return ZeebeClient.newClientBuilder()
        .brokerContactPoint(zeebe.getExternalAddress(ZeebePort.GATEWAY))
        .usePlaintext()
        .build();
  }

  public RecordRepository newRecordRepository() {
    final var streamer = new RecordRepository();
    ringBufferClient.addListener(new RecordRepositoryHazelcastAdapter(streamer));
    return streamer;
  }

  public HazelcastRingBufferClient newRingBufferClient() {
    return HazelcastRingBufferClient.builder()
        .withRingBufferName(HAZELCAST_RING_BUFFER_NAME)
        .withHazelcastAddress(hazelcast.getExternalAddress())
        .build();
  }

  private ZeebeBrokerContainer defaultZeebeContainer() {
    final var zeebeContainer =
        new ZeebeBrokerContainer("0.23.1")
            .withNetwork(network)
            .withNetworkAliases(ZEEBE_CONTAINER_ALIAS)
            .withDebug(false);
    return configureHazelcastExporter(zeebeContainer);
  }

  private HazelcastContainer defaultHazelcastContainer() {
    return new HazelcastContainer()
        .withNetwork(network)
        .withNetworkAliases(HAZELCAST_CONTAINER_ALIAS);
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
