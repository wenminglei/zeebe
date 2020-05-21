package io.zeebe.e2e.util.containers;

import io.zeebe.broker.system.configuration.ClusterCfg;
import io.zeebe.client.ZeebeClient;
import io.zeebe.containers.ZeebeBrokerContainer;
import io.zeebe.containers.ZeebeStandaloneGatewayContainer;
import io.zeebe.e2e.util.containers.configurators.ElasticsearchExporterConfigurator;
import io.zeebe.e2e.util.containers.elastic.ElasticExporterClient;
import io.zeebe.e2e.util.record.RecordRepository;
import io.zeebe.exporter.ElasticsearchExporterConfiguration;
import java.util.Map;
import java.util.Optional;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.rules.ExternalResource;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.lifecycle.Startable;

public class ZeebeElasticClusterRule extends ExternalResource {
  private static final String DEFAULT_EXPORTER_ID = "elastic";
  private static final String NETWORK_ALIAS = "elastic";
  private static final String DOCKER_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch";

  private final ZeebeClusterRule clusterRule;

  private ElasticsearchContainer elasticContainer;
  private ElasticsearchExporterConfiguration exporterConfig;
  private ElasticExporterClient exporterClient;
  private RecordRepository recordRepository;

  public ZeebeElasticClusterRule(final ZeebeClusterRule clusterRule) {
    this.clusterRule = clusterRule;
  }

  @Override
  public void before() throws Throwable {
    resolveExporterConfig();

    final var configurator =
        new ElasticsearchExporterConfigurator(DEFAULT_EXPORTER_ID, exporterConfig);
    clusterRule.withBrokerConfigurator(configurator);
    elasticContainer = newDefaultElasticContainer();

    elasticContainer.start();
    clusterRule.before();

    exporterClient = newElasticExporterClient();
    if (recordRepository == null) {
      recordRepository = new RecordRepository();
    }

    consumeExportedRecords();
  }

  @Override
  public void after() {
    Optional.ofNullable(exporterClient).ifPresent(ElasticExporterClient::stop);
    clusterRule.after();
    Optional.ofNullable(exporterClient).ifPresent(ElasticExporterClient::stop);
    Optional.ofNullable(elasticContainer).ifPresent(Startable::stop);
  }

  public ZeebeElasticClusterRule withExporterConfig(
      final ElasticsearchExporterConfiguration exporterConfig) {
    this.exporterConfig = exporterConfig;
    return this;
  }

  public ZeebeElasticClusterRule withRecordRepository(final RecordRepository recordRepository) {
    this.recordRepository = recordRepository;
    return this;
  }

  public ElasticsearchContainer getElasticContainer() {
    return elasticContainer;
  }

  public RecordRepository getRecordRepository() {
    return recordRepository;
  }

  public ZeebeClient getClient() {
    return clusterRule.getClient();
  }

  public ZeebeStandaloneGatewayContainer getGateway() {
    return clusterRule.getGateway();
  }

  public Map<Integer, ZeebeBrokerContainer> getBrokers() {
    return clusterRule.getBrokers();
  }

  public ZeebeBrokerContainer getBroker(final int nodeId) {
    return clusterRule.getBroker(nodeId);
  }

  public ClusterCfg getClusterConfig() {
    return clusterRule.getClusterConfig();
  }

  @SuppressWarnings("java:S2095")
  private ElasticsearchContainer newDefaultElasticContainer() {
    final var version = RestClient.class.getPackage().getImplementationVersion();
    final var logger = LoggerFactory.getLogger(ZeebeClusterRule.class + ".elastic");
    return new ElasticsearchContainer(DOCKER_IMAGE + ":" + version)
        .withNetwork(clusterRule.getNetwork())
        .withNetworkAliases(NETWORK_ALIAS)
        .withLogConsumer(new Slf4jLogConsumer(logger))
        .withEnv("discovery.type", "single-node");
  }

  private ElasticExporterClient newElasticExporterClient() {
    final var host = HttpHost.create(elasticContainer.getHttpHostAddress());
    final var client = RestClient.builder(host).build();

    return ElasticExporterClient.builder()
        .withClient(client)
        .withIndexPrefix(exporterConfig.index.prefix)
        .build();
  }

  private void consumeExportedRecords() {
    final var listener = new RecordRepositoryExporterClientListener(recordRepository);
    exporterClient.addListener(listener);
    exporterClient.start();
  }

  private void resolveExporterConfig() {
    final var config = Optional.ofNullable(exporterConfig).orElse(newDefaultExporterConfig());
    config.url = String.format("http://%s:9200", NETWORK_ALIAS);

    exporterConfig = config;
  }

  private ElasticsearchExporterConfiguration newDefaultExporterConfig() {
    final var config = new ElasticsearchExporterConfiguration();

    // set a very small delay/size to achieve something closer to streaming - terrible in production
    // but for testing should be alright unless stress testing
    config.bulk.delay = 1;
    config.bulk.size = 1;

    // ensure we export everything out by default
    config.index.command = true;
    config.index.event = true;
    config.index.rejection = true;
    config.index.deployment = true;
    config.index.error = true;
    config.index.incident = true;
    config.index.job = true;
    config.index.jobBatch = true;
    config.index.message = true;
    config.index.messageSubscription = true;
    config.index.variable = true;
    config.index.variableDocument = true;
    config.index.workflowInstance = true;
    config.index.workflowInstanceCreation = true;
    config.index.workflowInstanceSubscription = true;

    return config;
  }
}
