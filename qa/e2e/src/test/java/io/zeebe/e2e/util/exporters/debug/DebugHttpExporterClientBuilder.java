/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.e2e.util.exporters.debug;

import static io.zeebe.e2e.util.exporters.debug.DebugHttpExporterClient.DEFAULT_ERROR_HANDLER;

import io.grpc.netty.NettyChannelBuilder;

public final class DebugHttpExporterClientBuilder {
  private static final String DEFAULT_HOST = "localhost";
  private static final int DEFAULT_PORT = 8000;

  private String host = DEFAULT_HOST;
  private int port = DEFAULT_PORT;
  private RetryPredicate retryPredicate;

  public DebugHttpExporterClientBuilder withHost(final String host) {
    this.host = host;
    return this;
  }

  public DebugHttpExporterClientBuilder withPort(final int port) {
    this.port = port;
    return this;
  }

  public DebugHttpExporterClientBuilder withRetryPredicate(final RetryPredicate retryPredicate) {
    this.retryPredicate = retryPredicate;
    return this;
  }

  public DebugHttpExporterClient build() {
    if (host == null || host.isBlank()) {
      throw new IllegalArgumentException("Expected host to be something, but nothing given");
    }

    if (retryPredicate == null) {
      retryPredicate = DEFAULT_ERROR_HANDLER;
    }

    final var channel =
        NettyChannelBuilder.forAddress(host, port).enableRetry().usePlaintext().build();
    return new DebugHttpExporterClient(channel, retryPredicate);
  }
}
