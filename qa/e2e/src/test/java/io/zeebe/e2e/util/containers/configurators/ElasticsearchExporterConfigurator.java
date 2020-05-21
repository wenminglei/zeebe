package io.zeebe.e2e.util.containers.configurators;

import io.zeebe.containers.ZeebeBrokerContainer;
import io.zeebe.e2e.util.containers.BrokerConfigurator;
import io.zeebe.exporter.ElasticsearchExporter;
import io.zeebe.exporter.ElasticsearchExporterConfiguration;

public final class ElasticsearchExporterConfigurator implements BrokerConfigurator {
  private final String exporterId;
  private final ElasticsearchExporterConfiguration exporterConfig;

  public ElasticsearchExporterConfigurator(
      final String exporterId, final ElasticsearchExporterConfiguration exporterConfig) {
    this.exporterId = exporterId;
    this.exporterConfig = exporterConfig;
  }

  @Override
  public ZeebeBrokerContainer configure(final ZeebeBrokerContainer brokerContainer) {
    withEnv(brokerContainer, "CLASSNAME", ElasticsearchExporter.class.getName())
        .withEnv(brokerContainer, "ARGS_URL", exporterConfig.url)
        .withEnv(brokerContainer, "ARGS_INDEX_PREFIX", exporterConfig.index.prefix)
        .withEnv(brokerContainer, "ARGS_INDEX_COMMAND", exporterConfig.index.command)
        .withEnv(brokerContainer, "ARGS_INDEX_REJECTION", exporterConfig.index.rejection)
        .withEnv(brokerContainer, "ARGS_INDEX_JOBBATCH", exporterConfig.index.jobBatch)
        .withEnv(brokerContainer, "ARGS_INDEX_MESSAGE", exporterConfig.index.message)
        .withEnv(
            brokerContainer,
            "ARGS_INDEX_MESSAGESUBSCRIPTION",
            exporterConfig.index.messageSubscription)
        .withEnv(
            brokerContainer,
            "ARGS_INDEX_WORKFLOWINSTANCECREATION",
            exporterConfig.index.workflowInstanceCreation)
        .withEnv(
            brokerContainer,
            "ARGS_INDEX_WORKFLOWINSTANCESUBSCRIPTION",
            exporterConfig.index.workflowInstanceSubscription)
        .withEnv(brokerContainer, "ARGS_BULK_DELAY", exporterConfig.bulk.delay)
        .withEnv(brokerContainer, "ARGS_BULK_SIZE", exporterConfig.bulk.size);

    return brokerContainer;
  }

  private ElasticsearchExporterConfigurator withEnv(
      final ZeebeBrokerContainer container, final String suffix, final Object rawValue) {
    final var key =
        String.format(
            "ZEEBE_BROKER_EXPORTERS_%s_%s", exporterId.toUpperCase(), suffix.toUpperCase());
    final var value = String.valueOf(rawValue);

    container.withEnv(key, value);
    return this;
  }
}
