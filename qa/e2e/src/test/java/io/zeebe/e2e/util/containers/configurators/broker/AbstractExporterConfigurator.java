/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.e2e.util.containers.configurators.broker;

import io.zeebe.containers.ZeebeBrokerContainer;
import io.zeebe.e2e.util.containers.BrokerConfigurator;

abstract class AbstractExporterConfigurator<T extends AbstractExporterConfigurator<T>>
    implements BrokerConfigurator {
  protected final String exporterId;
  protected T myself;

  @SuppressWarnings("unchecked")
  AbstractExporterConfigurator(final String exporterId) {
    this.exporterId = exporterId;
    this.myself = (T) this;
  }

  protected T withEnv(
      final ZeebeBrokerContainer container, final String suffix, final Object rawValue) {
    final var key =
        String.format(
            "ZEEBE_BROKER_EXPORTERS_%s_%s", exporterId.toUpperCase(), suffix.toUpperCase());
    final var value = String.valueOf(rawValue);

    container.withEnv(key, value);
    return myself;
  }
}
