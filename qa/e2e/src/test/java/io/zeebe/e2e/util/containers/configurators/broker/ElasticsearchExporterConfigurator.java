/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.e2e.util.containers.configurators.broker;

import io.zeebe.containers.ZeebeBrokerContainer;
import io.zeebe.exporter.ElasticsearchExporter;
import io.zeebe.exporter.ElasticsearchExporterConfiguration;

public final class ElasticsearchExporterConfigurator
    extends AbstractExporterConfigurator<ElasticsearchExporterConfigurator> {
  private final ElasticsearchExporterConfiguration exporterConfig;

  public ElasticsearchExporterConfigurator(
      final String exporterId, final ElasticsearchExporterConfiguration exporterConfig) {
    super(exporterId);
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
}
