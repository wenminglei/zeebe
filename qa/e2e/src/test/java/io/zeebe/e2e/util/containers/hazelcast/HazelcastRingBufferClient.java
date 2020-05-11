/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.e2e.util.containers.hazelcast;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;
import io.zeebe.e2e.util.containers.ExporterClientListener;
import io.zeebe.e2e.util.containers.ObservableExporterClient;
import io.zeebe.protocol.immutables.record.RecordTypeReference;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HazelcastRingBufferClient implements ObservableExporterClient {
  private static final RecordTypeReference<?> TYPE_REFERENCE = new RecordTypeReference<>();
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Logger LOGGER = LoggerFactory.getLogger(HazelcastRingBufferClient.class);

  private final HazelcastInstance client;
  private final Ringbuffer<byte[]> ringBuffer;
  private final Set<ExporterClientListener> listeners;

  private long sequence;
  private ExecutorService executorService;
  private Future<?> future;

  public HazelcastRingBufferClient(
      final HazelcastInstance client, final Ringbuffer<byte[]> ringBuffer) {
    this(client, ringBuffer, ringBuffer.headSequence());
  }

  public HazelcastRingBufferClient(
      final HazelcastInstance client,
      final Ringbuffer<byte[]> ringBuffer,
      final long startSequence) {
    this.client = client;
    this.ringBuffer = ringBuffer;
    this.sequence = startSequence;

    this.listeners = new CopyOnWriteArraySet<>();
  }

  public HazelcastRingBufferClient(
      final HazelcastInstance client,
      final Ringbuffer<byte[]> ringBuffer,
      final long startSequence,
      final Collection<ExporterClientListener> initialListeners) {
    this(client, ringBuffer, startSequence);
    listeners.addAll(initialListeners);
  }

  public static HazelcastRingBufferClientBuilder builder() {
    return new HazelcastRingBufferClientBuilder();
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

    onExporterClientClose();
    client.shutdown();
  }

  @Override
  public Set<ExporterClientListener> getListeners() {
    return listeners;
  }

  @Override
  public Logger getLogger() {
    return LOGGER;
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
      onExporterClientRecord(record);
      sequence += 1;
    } catch (final IOException e) {
      LOGGER.error(
          "Failed to deserialize JSON entry of ring buffer {} at sequence {}",
          ringBuffer.getName(),
          sequence,
          e);
    }
  }
}
