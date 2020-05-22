/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.e2e.util.exporters.elastic;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

public final class ElasticExporterClientBuilder {
  private static final String DEFAULT_INDEX_PREFIX = "zeebe-record*";

  private RestClient client;
  private RestClientBuilder builder;
  private String indexPrefix = DEFAULT_INDEX_PREFIX;

  public ElasticExporterClientBuilder() {}

  public ElasticExporterClientBuilder withClient(final RestClient client) {
    this.client = client;
    return this;
  }

  public ElasticExporterClientBuilder withClientBuilder(final RestClientBuilder builder) {
    this.builder = builder;
    return this;
  }

  public ElasticExporterClientBuilder withIndexPrefix(final String indexPrefix) {
    this.indexPrefix = indexPrefix;
    return this;
  }

  public ElasticExporterClient build() {
    if (client == null) {
      if (builder == null) {
        throw new IllegalArgumentException(
            "Expected a builder to be present if no client given, but neither was present");
      }

      client = builder.build();
    }

    if (indexPrefix == null || indexPrefix.isBlank()) {
      indexPrefix = DEFAULT_INDEX_PREFIX;
    }

    if (!indexPrefix.endsWith("*")) {
      indexPrefix += "*";
    }

    return new ElasticExporterClient(client, indexPrefix);
  }
}
