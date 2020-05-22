/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.e2e.util.containers;

import io.zeebe.broker.system.configuration.ClusterCfg;
import io.zeebe.client.ZeebeClient;
import io.zeebe.containers.ZeebeBrokerContainer;
import io.zeebe.containers.ZeebePort;
import io.zeebe.containers.ZeebeStandaloneGatewayContainer;
import io.zeebe.e2e.util.ClusterInspector;
import io.zeebe.e2e.util.ClusterRule;
import io.zeebe.e2e.util.containers.configurators.BrokerConfiguratorChain;
import io.zeebe.e2e.util.containers.configurators.GatewayConfiguratorChain;
import io.zeebe.util.VersionUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.rules.ExternalResource;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;

public final class DockerClusterRule extends ExternalResource implements ClusterRule {
  private static final String DEFAULT_VERSION = VersionUtil.getPreviousVersion();

  private final String version;
  private final ClusterCfg clusterConfig;
  private final BrokerConfiguratorChain brokerConfigurators;
  private final GatewayConfiguratorChain gatewayConfigurators;
  private final Map<Integer, ZeebeBrokerContainer> brokers;
  private final DockerClusterInspector clusterInspector;

  private Network network;
  private ZeebeStandaloneGatewayContainer gateway;
  private ZeebeClient client;

  public DockerClusterRule() {
    this(DEFAULT_VERSION);
  }

  public DockerClusterRule(final String version) {
    this(version, new ClusterCfg());
  }

  public DockerClusterRule(final String version, final ClusterCfg clusterConfig) {
    this.version = version;
    this.clusterConfig = clusterConfig;

    this.clusterInspector = new DockerClusterInspector(this);
    this.brokerConfigurators = new BrokerConfiguratorChain();
    this.gatewayConfigurators = new GatewayConfiguratorChain();
    this.brokers = new HashMap<>();
  }

  @Override
  public void before() throws Throwable {
    if (network == null) {
      network = Network.newNetwork();
    }

    final var clusterSize = clusterConfig.getClusterSize();
    for (int i = 0; i < clusterSize; i++) {
      final var broker = createBroker(i);
      brokers.put(i, brokerConfigurators.configure(broker));
    }

    gateway = gatewayConfigurators.configure(createGateway(), brokers);

    final var startables = new ArrayList<Startable>(brokers.values());
    startables.add(gateway);
    Startables.deepStart(startables).join();

    client = newZeebeClient();
    clusterInspector.awaitCompleteGatewayTopology();
  }

  @Override
  public void after() {
    Optional.ofNullable(client).ifPresent(ZeebeClient::close);
    Optional.ofNullable(gateway).ifPresent(Startable::stop);
    brokers.values().stream().parallel().forEach(Startable::stop);
  }

  public DockerClusterRule withBrokerConfigurator(final BrokerConfigurator configurator) {
    brokerConfigurators.add(configurator);
    return this;
  }

  public DockerClusterRule withGatewayConfigurator(final GatewayConfigurator configurator) {
    gatewayConfigurators.add(configurator);
    return this;
  }

  public DockerClusterRule withNetwork(final Network network) {
    this.network = network;
    return this;
  }

  @Override
  public Network getNetwork() {
    return network;
  }

  @Override
  public ZeebeClient getClient() {
    return client;
  }

  @Override
  public ZeebeBrokerContainer getBroker(final int nodeId) {
    return brokers.get(nodeId);
  }

  @Override
  public Map<Integer, ZeebeBrokerContainer> getBrokers() {
    return brokers;
  }

  @Override
  public ZeebeStandaloneGatewayContainer getGateway() {
    return gateway;
  }

  @Override
  public ClusterCfg getClusterConfig() {
    return clusterConfig;
  }

  @Override
  public ClusterInspector getInspector() {
    return clusterInspector;
  }

  private ZeebeBrokerContainer createBroker(final int nodeId) {
    final var logger =
        LoggerFactory.getLogger(DockerClusterRule.class.getName() + ".brokers." + nodeId);
    final var contactPoints = generateContactPoints();
    return newDefaultBroker()
        .withEnv("ZEEBE_BROKER_CLUSTER_NODEID", String.valueOf(nodeId))
        .withEnv("ZEEBE_BROKER_CLUSTER_CLUSTERNAME", clusterConfig.getClusterName())
        .withEnv("ZEEBE_BROKER_CLUSTER_INITIALCONTACTPOINTS", String.join(",", contactPoints))
        .withEnv(
            "ZEEBE_BROKER_CLUSTER_PARTITIONSCOUNT",
            String.valueOf(clusterConfig.getPartitionsCount()))
        .withEnv(
            "ZEEBE_BROKER_CLUSTER_REPLICATIONFACTOR",
            String.valueOf(clusterConfig.getReplicationFactor()))
        .withEnv("ZEEBE_BROKER_CLUSTER_CLUSTERSIZE", String.valueOf(clusterConfig.getClusterSize()))
        .withEnv("ZEEBE_BROKER_NETWORK_HOST", "0.0.0.0")
        .withEnv("ZEEBE_BROKER_NETWORK_ADVERTISEDHOST", getBrokerNetworkAlias(nodeId))
        .withNetwork(network)
        .withNetworkAliases(getBrokerNetworkAlias(nodeId))
        .withLogConsumer(new Slf4jLogConsumer(logger, true));
  }

  private ZeebeStandaloneGatewayContainer createGateway() {
    final var logger = LoggerFactory.getLogger(DockerClusterRule.class.getName() + ".gateway");
    return newDefaultGateway()
        .withEnv("ZEEBE_GATEWAY_CLUSTER_MEMBERID", "gateway")
        .withEnv("ZEEBE_GATEWAY_CLUSTER_CLUSTERNAME", clusterConfig.getClusterName())
        .withEnv("ZEEBE_GATEWAY_CLUSTER_CONTACTPOINT", generateContactPoint(0))
        .withEnv("ZEEBE_GATEWAY_THREADS_MANAGEMENTTHREADS", String.valueOf(4))
        .withNetwork(network)
        .withNetworkAliases("gateway")
        .withLogConsumer(new Slf4jLogConsumer(logger, true));
  }

  private ZeebeBrokerContainer newDefaultBroker() {
    return new ZeebeBrokerContainer(version);
  }

  private ZeebeStandaloneGatewayContainer newDefaultGateway() {
    return new ZeebeStandaloneGatewayContainer(version);
  }

  private String getBrokerNetworkAlias(final int nodeId) {
    return "broker-" + nodeId;
  }

  private ZeebeClient newZeebeClient() {
    return ZeebeClient.newClientBuilder()
        .brokerContactPoint(gateway.getExternalAddress(ZeebePort.GATEWAY))
        .usePlaintext()
        .build();
  }

  private List<String> generateContactPoints() {
    final var clusterSize = clusterConfig.getClusterSize();
    final var contactPoints = new ArrayList<String>(clusterSize);
    for (int i = 0; i < clusterSize; i++) {
      contactPoints.add(i, generateContactPoint(i));
    }

    return contactPoints;
  }

  private String generateContactPoint(final int nodeId) {
    return getBrokerNetworkAlias(nodeId) + ":26502";
  }
}
