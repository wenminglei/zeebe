package io.zeebe.e2e.util.containers.elastic;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.zeebe.e2e.util.containers.ExporterClientListener;
import io.zeebe.e2e.util.containers.ObservableExporterClient;
import io.zeebe.protocol.immutables.record.RecordTypeReference;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ElasticScrollClient implements ObservableExporterClient {
  private static final RecordTypeReference<?> TYPE_REFERENCE = new RecordTypeReference<>();
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticScrollClient.class);

  private final URI endpoint;
  private final HttpClient client;
  private final Set<ExporterClientListener> listeners;

  private ExecutorService executorService;
  private Future<?> consumeTask;
  private String scrollId;

  public ElasticScrollClient(final HttpClient client, final URI endpoint) {
    this.client = client;
    this.endpoint = endpoint;

    this.listeners = new CopyOnWriteArraySet<>();
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
  }

  public void stop() {
    LOGGER.info("Closing Elastic scroll client");

    if (consumeTask != null) {
      consumeTask.cancel(true);
    }

    if (executorService != null) {
      executorService.shutdown();
    }

    onExporterClientClose();
  }

  private Future<?> consumeRecords() {
    final var thread = Thread.currentThread();

    while (!thread.isInterrupted()) {
      try {
        handleRecord();
      } catch (final InterruptedException e) {
        LOGGER.debug("Interrupted while consuming records");
        thread.interrupt();
      }
    }
  }

  private void handleRecord(final String jsonEntry) {
    try {
      final var record = MAPPER.readValue(jsonEntry, TYPE_REFERENCE);
      onExporterClientRecord(record);
    } catch (final IOException e) {
      LOGGER.error("Failed to deserialize JSON entry", e);
    }
  }

  @JsonInclude(Include.NON_EMPTY)
  private static final class ScrollRequest {
    private final int size;
    private final String scroll;
    private final String scrollId;

    public ScrollRequest(final int size, final String scroll, final String scrollId) {
      this.size = size;
      this.scroll = scroll;
      this.scrollId = scrollId;
    }

    public ScrollRequest() {
      this(100, "5m", null);
    }
  }

  private static final class ScrollResponse {
    private String scrollId;
    
  }
}
