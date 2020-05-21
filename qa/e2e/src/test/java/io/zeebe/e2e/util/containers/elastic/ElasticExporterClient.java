/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.e2e.util.containers.elastic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.zeebe.e2e.util.containers.ExporterClientListener;
import io.zeebe.e2e.util.containers.ObservableExporterClient;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ElasticExporterClient implements ObservableExporterClient {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticExporterClient.class);
  private static final int SEARCH_SIZE = 100;

  // 100ms * 100 = 10s effective retry
  private static final int MAX_RETRIES = 100;
  private static final int RETRY_BACKOFF_MS = 100;

  private final RestClient client;
  private final String endpoint;
  private final Set<ExporterClientListener> listeners;

  private ExecutorService executorService;
  private Future<?> consumeTask;
  private int unavailableRetriesCounter = 0;

  public ElasticExporterClient(final RestClient client, final String indexPrefix) {
    this.client = client;
    this.listeners = new CopyOnWriteArraySet<>();

    final var indexFilter = URLEncoder.encode(indexPrefix, StandardCharsets.UTF_8);
    this.endpoint = String.format("/%s/_search", indexFilter);
  }

  public static ElasticExporterClientBuilder builder() {
    return new ElasticExporterClientBuilder();
  }

  @Override
  public Set<ExporterClientListener> getListeners() {
    return listeners;
  }

  @Override
  public Logger getLogger() {
    return LOGGER;
  }

  public void start() {
    executorService = Executors.newSingleThreadExecutor();
    consumeTask = executorService.submit(this::consumeRecords);

    LOGGER.info("Started elastic exporter client on {}", client.getNodes());
  }

  public void stop() {
    LOGGER.info("Closing elastic exporter client...");

    if (consumeTask != null) {
      consumeTask.cancel(true);
    }

    if (executorService != null) {
      executorService.shutdown();
    }

    onExporterClientClose();
  }

  private void consumeRecords() {
    final var thread = Thread.currentThread();
    final var mutableSearchQuery = new SearchRequestDto(SEARCH_SIZE);

    while (!thread.isInterrupted()) {
      final var request = new Request("POST", endpoint);

      try {
        request.setJsonEntity(MAPPER.writeValueAsString(mutableSearchQuery));
      } catch (final JsonProcessingException e) {
        LOGGER.error("Failed to serialize search query, aborting...", e);
        return;
      }

      Optional<SearchResponseDto> searchResponse = Optional.empty();
      try {
        searchResponse = search(request);
      } catch (final JsonProcessingException e) {
        LOGGER.error("Failed to parse search response, retrying...", e);
      } catch (final InterruptedException ex) {
        thread.interrupt();
      } catch (final IOException e) {
        LOGGER.error("Failed to perform search query against server, retrying...", e);
      }

      searchResponse.ifPresent(r -> handleRecords(r, mutableSearchQuery));
    }
  }

  private void handleRecords(
      final SearchResponseDto searchResponse, final SearchRequestDto mutableSearchQuery) {
    final var documents = searchResponse.getDocuments();
    for (final var document : documents) {
      onExporterClientRecord(document.getRecord());
      mutableSearchQuery.setSearchCursor(document.getRecord().getTimestamp(), document.getId());
    }
  }

  private Optional<SearchResponseDto> search(final Request request)
      throws InterruptedException, IOException {
    try {
      final var response = client.performRequest(request);
      unavailableRetriesCounter = 0;

      final var searchResponseDto =
          MAPPER.readValue(response.getEntity().getContent(), SearchResponseDto.class);
      return Optional.of(searchResponseDto);
    } catch (final ResponseException e) {
      // it's possible that Elastic is still starting, which accounts for the 503s that we get...
      // in this case, simply retry with a backoff. Another option would be to wait for the indices
      // to exist before we start querying, as this occurs when the indices are being created.
      if (e.getResponse().getStatusLine().getStatusCode() != 503
          || unavailableRetriesCounter >= MAX_RETRIES) {
        throw e;
      }

      unavailableRetriesCounter++;
      Thread.sleep(RETRY_BACKOFF_MS);
    }

    return Optional.empty();
  }
}
