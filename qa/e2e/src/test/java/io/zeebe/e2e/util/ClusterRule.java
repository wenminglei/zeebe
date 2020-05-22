/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.e2e.util;

import io.zeebe.broker.system.configuration.ClusterCfg;
import io.zeebe.client.ZeebeClient;
import io.zeebe.containers.ZeebeBrokerContainer;
import io.zeebe.containers.ZeebeStandaloneGatewayContainer;
import java.util.Map;
import org.junit.rules.TestRule;

public interface ClusterRule extends TestRule {
  ZeebeClient getClient();

  ZeebeBrokerContainer getBroker(final int nodeId);

  Map<Integer, ZeebeBrokerContainer> getBrokers();

  ZeebeStandaloneGatewayContainer getGateway();

  ClusterCfg getClusterConfig();

  ClusterInspector getInspector();
}
