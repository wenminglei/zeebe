/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.e2e.util.containers;

import static org.awaitility.Awaitility.await;

import io.zeebe.client.api.response.BrokerInfo;
import io.zeebe.client.api.response.PartitionInfo;
import io.zeebe.client.api.response.Topology;
import io.zeebe.e2e.util.ClusterInspector;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DockerClusterInspector implements ClusterInspector {

  private final DockerClusterRule clusterRule;

  public DockerClusterInspector(final DockerClusterRule clusterRule) {
    this.clusterRule = clusterRule;
  }

  @Override
  public Integer getLeaderForPartition(final int partitionId) {
    return await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> tryGetLeaderForPartition(partitionId), Optional::isPresent)
        .orElseThrow();
  }

  @Override
  public void awaitCompleteGatewayTopology() {
    await().atMost(Duration.ofSeconds(10)).until(this::isGatewayTopologyComplete);
  }

  @Override
  public Map<Integer, Integer> getLeaders() {
    final var topology = getTopology();
    final var brokers = topology.getBrokers();
    final var leaders = new HashMap<Integer, Integer>();

    for (final var broker : brokers) {
      broker.getPartitions().stream()
          .filter(PartitionInfo::isLeader)
          .map(PartitionInfo::getPartitionId)
          .forEach(id -> leaders.put(id, broker.getNodeId()));
    }

    return leaders;
  }

  private Optional<Integer> tryGetLeaderForPartition(final int partitionId) {
    final var brokers = getTopology().getBrokers();

    return brokers.stream()
        .filter(broker -> isLeaderForPartitionId(broker, partitionId))
        .findFirst()
        .map(BrokerInfo::getNodeId);
  }

  private boolean isLeaderForPartitionId(final BrokerInfo broker, final int partitionId) {
    return broker.getPartitions().stream()
        .anyMatch(p -> p.isLeader() && p.getPartitionId() == partitionId);
  }

  private boolean isGatewayTopologyComplete() {
    final var brokers = getTopology().getBrokers();
    final var clusterConfig = clusterRule.getClusterConfig();

    return brokers.size() == clusterConfig.getClusterSize()
        && brokers.stream()
            .allMatch(b -> b.getPartitions().size() == clusterConfig.getPartitionsCount());
  }

  private Topology getTopology() {
    final var client = clusterRule.getClient();
    return client.newTopologyRequest().requestTimeout(Duration.ofSeconds(10)).send().join();
  }
}
