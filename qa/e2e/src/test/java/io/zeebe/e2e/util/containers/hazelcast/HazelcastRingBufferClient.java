package io.zeebe.e2e.util.containers.hazelcast;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;
import io.zeebe.protocol.immutables.record.RecordTypeReference;
import io.zeebe.protocol.record.Record;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HazelcastRingBufferClient {
  private static final RecordTypeReference TYPE_REFERENCE = new RecordTypeReference();
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Logger LOGGER = LoggerFactory.getLogger(HazelcastRingBufferClient.class);

  private final HazelcastInstance client;
  private final Ringbuffer<byte[]> ringBuffer;
  private final Set<Listener> listeners;

  private long sequence;
  private ExecutorService executorService;
  private Future<?> future;

  public HazelcastRingBufferClient(final HazelcastInstance client, final Ringbuffer<byte[]> ringBuffer) {
    this(client, ringBuffer, ringBuffer.headSequence());
  }

  public HazelcastRingBufferClient(final HazelcastInstance client, final Ringbuffer<byte[]> ringBuffer, final long startSequence) {
    this.client = client;
    this.ringBuffer = ringBuffer;
    this.sequence = startSequence;

    this.listeners = new CopyOnWriteArraySet<>();
  }

  public HazelcastRingBufferClient(
      final HazelcastInstance client,
      final Ringbuffer<byte[]> ringBuffer,
      final long startSequence,
      final Collection<Listener> initialListeners) {
    this(client, ringBuffer, startSequence);
    listeners.addAll(initialListeners);
  }

  public static HazelcastRingBufferClientBuilder builder() {
    return new HazelcastRingBufferClientBuilder();
  }

  public void addListener(final Listener listener) {
    listeners.add(listener);
  }

  public void removeListener(final Listener listener) {
    listeners.remove(listener);
  }

  public void start() {
    executorService = Executors.newSingleThreadExecutor();
    future = executorService.submit(this::consumeEndlessly);
  }

  public void stop() {
    LOGGER.info("Closing ring buffer client, current sequence: '{}'", getSequence());

    if (future != null) {
      future.cancel(true);
    }

    if (executorService != null) {
      executorService.shutdown();
    }

    for (final var listener : listeners) {
      safelyCloseListener(listener);
    }

    client.shutdown();
  }

  public long getSequence() {
    return sequence;
  }

  private void consumeEndlessly() {
    final var thread = Thread.currentThread();

    while (!thread.isInterrupted()) {
      try {
        consumeRecord();
      } catch (final InterruptedException e) {
        LOGGER.debug(
            "Interrupted while consuming ring buffer {} at sequence {}",
            ringBuffer.getName(),
            sequence);
        thread.interrupt();
      }
    }
  }

  private void consumeRecord() throws InterruptedException {
    try {
      final var jsonEntry = ringBuffer.readOne(sequence);
      final var record = MAPPER.readValue(jsonEntry, TYPE_REFERENCE);
      for (final var listener : listeners) {
        safelyNotifyListener(listener, record);
      }

      sequence += 1;
    } catch (final IOException e) {
      LOGGER.error(
          "Failed to deserialize JSON entry of ring buffer {} at sequence {}",
          ringBuffer.getName(),
          sequence,
          e);
    }
  }

  private void safelyNotifyListener(final Listener listener, final Record<?> record) {
    try {
      listener.onRecord(record);
    } catch (final Exception e) {
      LOGGER.error(
          "Unexpected error notify Hazelcast ring buffer client listener {} with record {}",
          listener,
          record,
          e);
    }
  }

  private void safelyCloseListener(final Listener listener) {
    try {
      listener.onClose();
    } catch (final Exception e) {
      LOGGER.error(
          "Unexpected error closing Hazelcast ring buffer client listener {}", listener, e);
    }
  }

  @FunctionalInterface
  public interface Listener {
    void onRecord(final Record<?> record);

    default void onClose() {}
  }
}
