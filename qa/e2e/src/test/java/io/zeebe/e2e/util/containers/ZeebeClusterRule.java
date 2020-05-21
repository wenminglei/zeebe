package io.zeebe.e2e.util.containers;

import static org.awaitility.Awaitility.await;

import io.zeebe.broker.system.configuration.ClusterCfg;
import io.zeebe.client.ZeebeClient;
import io.zeebe.containers.ZeebeBrokerContainer;
import io.zeebe.containers.ZeebePort;
import io.zeebe.containers.ZeebeStandaloneGatewayContainer;
import io.zeebe.e2e.util.containers.configurators.BrokerConfiguratorChain;
import io.zeebe.e2e.util.containers.configurators.GatewayConfiguratorChain;
import io.zeebe.util.VersionUtil;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.agrona.collections.Int2ObjectHashMap;
import org.jetbrains.annotations.NotNull;
import org.junit.rules.ExternalResource;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;

public final class ZeebeClusterRule extends ExternalResource {
  private static final String DEFAULT_VERSION = VersionUtil.getPreviousVersion();

  private final String version;
  private final ClusterCfg clusterConfig;
  private final BrokerConfiguratorChain brokerConfigurators;
  private final GatewayConfiguratorChain gatewayConfigurators;
  private final Int2ObjectHashMap<ZeebeBrokerContainer> brokers;

  private Network network;
  private ZeebeStandaloneGatewayContainer gateway;
  private ZeebeClient client;

  public ZeebeClusterRule() {
    this(DEFAULT_VERSION);
  }

  public ZeebeClusterRule(final String version) {
    this(version, new ClusterCfg());
  }

  public ZeebeClusterRule(final String version, final ClusterCfg clusterConfig) {
    this.version = version;
    this.clusterConfig = clusterConfig;

    this.brokerConfigurators = new BrokerConfiguratorChain();
    this.gatewayConfigurators = new GatewayConfiguratorChain();
    this.brokers = new Int2ObjectHashMap<>();
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
    await().atMost(Duration.ofSeconds(10)).until(this::isGatewayReadyForClient);
  }

  @Override
  public void after() {
    Optional.ofNullable(client).ifPresent(ZeebeClient::close);
    Optional.ofNullable(gateway).ifPresent(Startable::stop);
    brokers.values().stream().parallel().forEach(Startable::stop);
  }

  public ZeebeClusterRule withBrokerConfigurator(final BrokerConfigurator configurator) {
    brokerConfigurators.add(configurator);
    return this;
  }

  public ZeebeClusterRule withGatewayConfigurator(final GatewayConfigurator configurator) {
    gatewayConfigurators.add(configurator);
    return this;
  }

  public ZeebeClusterRule withNetwork(final Network network) {
    this.network = network;
    return this;
  }

  public ZeebeClient getClient() {
    return client;
  }

  public Network getNetwork() {
    return network;
  }

  public ZeebeStandaloneGatewayContainer getGateway() {
    return gateway;
  }

  public Map<Integer, ZeebeBrokerContainer> getBrokers() {
    return brokers;
  }

  public ZeebeBrokerContainer getBroker(final int nodeId) {
    return brokers.get(nodeId);
  }

  public ClusterCfg getClusterConfig() {
    return clusterConfig;
  }

  private ZeebeBrokerContainer createBroker(final int nodeId) {
    final var logger = LoggerFactory.getLogger(ZeebeClusterRule.class + ".brokers." + nodeId);
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
        .withLogConsumer(new Slf4jLogConsumer(logger));
  }

  private ZeebeStandaloneGatewayContainer createGateway() {
    final var logger = LoggerFactory.getLogger(ZeebeClusterRule.class + ".gateway");
    return newDefaultGateway()
        .withEnv("ZEEBE_GATEWAY_CLUSTER_MEMBERID", "gateway")
        .withEnv("ZEEBE_GATEWAY_CLUSTER_CLUSTERNAME", clusterConfig.getClusterName())
        .withEnv("ZEEBE_GATEWAY_CLUSTER_CONTACTPOINT", generateContactPoint(0))
        .withEnv("ZEEBE_GATEWAY_THREADS_MANAGEMENTTHREADS", String.valueOf(4))
        .withNetwork(network)
        .withNetworkAliases("gateway")
        .withLogConsumer(new Slf4jLogConsumer(logger));
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

  @NotNull
  private String generateContactPoint(final int nodeId) {
    return getBrokerNetworkAlias(nodeId) + ":26502";
  }

  private boolean isGatewayReadyForClient() {
    final var topology =
        getClient().newTopologyRequest().requestTimeout(Duration.ofSeconds(10)).send().join();
    final var topologyBrokers = topology.getBrokers();

    return topologyBrokers.size() == clusterConfig.getClusterSize()
        && topologyBrokers.stream()
            .allMatch(b -> b.getPartitions().size() == clusterConfig.getPartitionsCount());
  }
}
