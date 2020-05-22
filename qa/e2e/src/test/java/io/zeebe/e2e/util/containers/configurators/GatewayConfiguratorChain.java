/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.e2e.util.containers.configurators;

import io.zeebe.containers.ZeebeBrokerContainer;
import io.zeebe.containers.ZeebeStandaloneGatewayContainer;
import io.zeebe.e2e.util.containers.GatewayConfigurator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class GatewayConfiguratorChain implements GatewayConfigurator {
  private final List<GatewayConfigurator> configurators;

  public GatewayConfiguratorChain(final GatewayConfigurator... configurators) {
    this(Arrays.asList(configurators));
  }

  public GatewayConfiguratorChain(final List<GatewayConfigurator> configurators) {
    this.configurators = new ArrayList<>();
    this.configurators.addAll(configurators);
  }

  public void add(final GatewayConfigurator configurator) {
    configurators.add(configurator);
  }

  @Override
  public ZeebeStandaloneGatewayContainer configure(
      final ZeebeStandaloneGatewayContainer gatewayContainer,
      final Map<Integer, ZeebeBrokerContainer> brokers) {
    var container = gatewayContainer;
    for (final var configurator : configurators) {
      container = configurator.configure(container, brokers);
    }

    return container;
  }
}
