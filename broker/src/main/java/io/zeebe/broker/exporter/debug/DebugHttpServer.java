/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.exporter.debug;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.zeebe.broker.exporter.debug.protocol.ExporterGrpc;
import io.zeebe.broker.exporter.debug.protocol.ExporterOuterClass.FetchRecordsRequest;
import io.zeebe.broker.exporter.debug.protocol.ExporterOuterClass.JsonRecord;
import io.zeebe.protocol.record.Record;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class DebugHttpServer extends ExporterGrpc.ExporterImplBase {

  private final InetSocketAddress address;
  private final int maxSize;
  private final Lock recordsLock;
  private final Condition recordsSignalCondition;
  private final LinkedList<Record> records;

  private Server server;

  public DebugHttpServer(final String host, final int port, final int maxSize) {
    this.address = new InetSocketAddress(host, port);
    this.maxSize = maxSize;

    this.recordsLock = new ReentrantLock();
    this.recordsSignalCondition = recordsLock.newCondition();
    this.records = new LinkedList<>();
  }

  public void start() {
    stop();

    server = NettyServerBuilder.forAddress(address).addService(this).build();

    try {
      server.start();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public void close() {
    stop();
    records.clear();
  }

  public void stop() {
    if (server != null) {
      server.shutdownNow();
      try {
        server.awaitTermination(10, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      server = null;
    }
  }

  public void add(final Record record) {
    try {
      recordsLock.lockInterruptibly();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }

    try {
      if (records.size() >= maxSize) {
        records.removeFirst();
      }

      records.add(record.clone());
      recordsSignalCondition.signalAll();
    } finally {
      recordsLock.unlock();
    }
  }

  @Override
  public void fetchRecords(
      final FetchRecordsRequest request, final StreamObserver<JsonRecord> responseObserver) {
    final var thread = Thread.currentThread();
    var index = 0;

    while (!thread.isInterrupted()) {
      try {
        recordsLock.lockInterruptibly();
      } catch (final InterruptedException e) {
        thread.interrupt();
        responseObserver.onCompleted();
        return;
      }

      final Record record;
      try {
        while (index >= records.size()) {
          try {
            recordsSignalCondition.await(5, TimeUnit.SECONDS);
          } catch (final InterruptedException e) {
            thread.interrupt();
            responseObserver.onCompleted();
            return;
          }
        }

        record = records.get(index++);
      } finally {
        recordsLock.unlock();
      }

      responseObserver.onNext(JsonRecord.newBuilder().setSerialized(record.toJson()).build());
    }
  }
}
