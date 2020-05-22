/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.e2e.util.containers.configurators.broker;

import io.zeebe.broker.exporter.debug.DebugHttpExporter;
import io.zeebe.containers.ZeebeBrokerContainer;

public final class DebugHttpExporterConfigurator
    extends AbstractExporterConfigurator<DebugHttpExporterConfigurator> {

  private static final String DEFAULT_EXPORTER_ID = "debugHttp";
  private static final int DEFAULT_EXPORTER_PORT = 8000;

  private final int port;

  public DebugHttpExporterConfigurator() {
    this(DEFAULT_EXPORTER_ID, DEFAULT_EXPORTER_PORT);
  }

  public DebugHttpExporterConfigurator(final int port) {
    this(DEFAULT_EXPORTER_ID, port);
  }

  public DebugHttpExporterConfigurator(final String exporterId, final int port) {
    super(exporterId);
    this.port = port;
  }

  @Override
  public ZeebeBrokerContainer configure(final ZeebeBrokerContainer brokerContainer) {
    withEnv(brokerContainer, "CLASSNAME", DebugHttpExporter.class.getName())
        .withEnv(brokerContainer, "ARGS_PORT", String.valueOf(port));
    brokerContainer.addExposedPorts(port);

    return brokerContainer;
  }
}
