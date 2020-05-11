/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.e2e.util.containers.debug;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Context;
import io.grpc.Context.CancellableContext;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import io.zeebe.broker.exporter.debug.protocol.ExporterGrpc;
import io.zeebe.broker.exporter.debug.protocol.ExporterOuterClass.FetchRecordsRequest;
import io.zeebe.broker.exporter.debug.protocol.ExporterOuterClass.JsonRecord;
import io.zeebe.e2e.util.containers.ExporterClientListener;
import io.zeebe.e2e.util.containers.ObservableExporterClient;
import io.zeebe.protocol.immutables.record.RecordTypeReference;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DebugHttpExporterClient implements ObservableExporterClient {
  private static final RecordTypeReference<?> TYPE_REFERENCE = new RecordTypeReference<>();
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Logger LOGGER = LoggerFactory.getLogger(DebugHttpExporterClient.class);

  private final ExporterGrpc.ExporterStub service;
  private final Set<ExporterClientListener> listeners;

  private ExecutorService executorService;
  private CancellableContext grpcContext;

  public DebugHttpExporterClient(final ManagedChannel channel) {
    this.service = ExporterGrpc.newStub(channel).withExecutor(executorService).withWaitForReady();
    this.listeners = new CopyOnWriteArraySet<>();
  }

  public static DebugHttpExporterClientBuilder builder() {
    return new DebugHttpExporterClientBuilder();
  }

  public void start() {
    executorService = Executors.newSingleThreadExecutor();
    grpcContext = Context.current().withCancellation();
    grpcContext.run(this::consumeRecords);
  }

  public void stop() {
    if (grpcContext != null) {
      grpcContext.cancel(null);
    }

    if (executorService != null) {
      executorService.shutdownNow();
      try {
        executorService.awaitTermination(10, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    onExporterClientClose();
  }

  @Override
  public Set<ExporterClientListener> getListeners() {
    return listeners;
  }

  @Override
  public void addListener(final ExporterClientListener listener) {
    this.listeners.add(listener);
  }

  @Override
  public void removeListener(final ExporterClientListener listener) {
    this.listeners.remove(listener);
  }

  @Override
  public Logger getLogger() {
    return LOGGER;
  }

  private void consumeRecords() {
    final var request = FetchRecordsRequest.newBuilder().build();
    service
        .withDeadlineAfter(30, TimeUnit.SECONDS)
        .fetchRecords(
            request,
            new StreamObserver<>() {
              @Override
              public void onNext(final JsonRecord jsonRecord) {
                LOGGER.trace("Fetched JSON record from debug exporter server {}", jsonRecord);
                handleRecord(jsonRecord.getSerialized());
              }

              @Override
              public void onError(final Throwable throwable) {
                LOGGER.error("Debug exporter server returned an unexpected error", throwable);
              }

              @Override
              public void onCompleted() {
                LOGGER.debug("Debug exporter server closed the connection");
              }
            });
  }

  private void handleRecord(final String jsonEntry) {
    try {
      final var record = MAPPER.readValue(jsonEntry, TYPE_REFERENCE);
      onExporterClientRecord(record);
    } catch (final IOException e) {
      LOGGER.error("Failed to deserialize JSON entry {}", jsonEntry, e);
    }
  }
}
