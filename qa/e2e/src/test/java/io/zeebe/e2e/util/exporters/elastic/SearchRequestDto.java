/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.e2e.util.exporters.elastic;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@JsonInclude(Include.NON_EMPTY)
public final class SearchRequestDto {

  private static final List<Map<String, String>> SORT_ORDER =
      List.of(Map.of("timestamp", "asc"), Map.of("_id", "asc"));
  private static final List<String> STORED_FIELDS = List.of("_source", "_id");

  @JsonProperty("size")
  private final int size;

  @JsonProperty("sort")
  private final List<Map<String, String>> sortOptions;

  @JsonProperty("stored_fields")
  private final List<String> fields;

  @JsonProperty("search_after")
  private List<Object> searchCursor;

  public SearchRequestDto(final int size) {
    this.size = size;

    this.searchCursor = Collections.emptyList();
    this.sortOptions = SORT_ORDER;
    this.fields = STORED_FIELDS;
  }

  public int getSize() {
    return size;
  }

  public void setSearchCursor(final long lastTimestamp, final String lastId) {
    searchCursor = List.of(lastTimestamp, lastId);
  }
}
