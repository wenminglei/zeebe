/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.e2e.util.containers;

import io.grpc.Status.Code;
import io.zeebe.client.api.command.ClientStatusException;
import io.zeebe.containers.ZeebeBrokerContainer;
import io.zeebe.e2e.util.ClusterRule;
import io.zeebe.e2e.util.DelegatingClusterRule;
import io.zeebe.e2e.util.StreamableClusterRule;
import io.zeebe.e2e.util.containers.configurators.broker.DebugHttpExporterConfigurator;
import io.zeebe.e2e.util.exporters.ExporterClient;
import io.zeebe.e2e.util.exporters.debug.DebugHttpExporterClient;
import io.zeebe.e2e.util.record.RecordRepository;
import io.zeebe.e2e.util.record.RecordRepositoryExporterClientListener;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.LockSupport;
import org.junit.rules.ExternalResource;
import org.testcontainers.containers.Network;

public class DockerDebugHttpStreambleClusterRule extends ExternalResource
    implements StreamableClusterRule, DelegatingClusterRule {

  private static final int DEFAULT_EXPORTER_PORT = 8000;
  private final DockerClusterRule clusterRule;

  private int port = DEFAULT_EXPORTER_PORT;
  private Map<Integer, DebugHttpExporterClient> exporterClients;
  private RecordRepository recordRepository;
  private RecordRepositoryExporterClientListener exporterClientListener;

  public DockerDebugHttpStreambleClusterRule(final DockerClusterRule clusterRule) {
    this.clusterRule = clusterRule;
    this.exporterClients = new HashMap<>();
  }

  @Override
  public void before() throws Throwable {
    resolveNetwork();

    final var configurator = new DebugHttpExporterConfigurator(port);
    clusterRule.withBrokerConfigurator(configurator);

    clusterRule.before();

    if (recordRepository == null) {
      recordRepository = new RecordRepository();
    }
    exporterClientListener = new RecordRepositoryExporterClientListener(recordRepository);

    startInitialExporterClients();
  }

  @Override
  public void after() {
    exporterClients.values().forEach(ExporterClient::stop);
    exporterClients.clear();
    Optional.ofNullable(recordRepository).ifPresent(RecordRepository::reset);
    clusterRule.after();
  }

  public DockerDebugHttpStreambleClusterRule withPort(final int port) {
    this.port = port;
    return this;
  }

  public DockerDebugHttpStreambleClusterRule withRecordRepository(
      final RecordRepository recordRepository) {
    this.recordRepository = recordRepository;
    return this;
  }

  @Override
  public RecordRepository getRecordRepository() {
    return recordRepository;
  }

  @Override
  public ClusterRule getClusterRuleDelegate() {
    return clusterRule;
  }

  private void resolveNetwork() {
    final var network = clusterRule.getNetwork();
    if (network == null) {
      clusterRule.withNetwork(Network.newNetwork());
    }
  }

  private DebugHttpExporterClient newExporterClient(final ZeebeBrokerContainer broker) {
    return DebugHttpExporterClient.builder()
        .withHost("localhost")
        .withPort(broker.getMappedPort(port))
        .withRetryPredicate(this::shouldRetryOnExporterClientError)
        .build();
  }

  private boolean shouldRetryOnExporterClientError(final Throwable error) {
    long backOffNs = 250_000;
    if (error instanceof ClientStatusException) {
      final var statusException = (ClientStatusException) error;
      if (statusException.getStatusCode() == Code.UNAVAILABLE) {
        backOffNs = 1_000_000;
      }
    }

    LockSupport.parkNanos(backOffNs);
    return true;
  }

  private void startInitialExporterClients() {
    final var brokers = getClusterRuleDelegate().getBrokers();
    for (final var broker : brokers.entrySet()) {
      startExporterClient(broker.getKey(), broker.getValue());
    }
  }

  private void startExporterClient(final Integer nodeId, final ZeebeBrokerContainer broker) {
    final var newClient = newExporterClient(broker);
    exporterClients.put(nodeId, newClient);
    newClient.addListener(exporterClientListener);
    newClient.start();
  }
}
