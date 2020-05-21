/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.e2e.util.containers.elastic;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.zeebe.protocol.immutables.record.ImmutableRecord;
import io.zeebe.protocol.record.Record;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class SearchResponseDto {
  @JsonProperty(value = "hits", required = true)
  private DocumentsWrapper documentsWrapper;

  public List<Document> getDocuments() {
    return documentsWrapper.documents;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static final class Document {
    @JsonProperty(value = "_index", required = true)
    private String index;

    @JsonProperty(value = "_id", required = true)
    private String id;

    @JsonProperty(value = "_source", required = true)
    private ImmutableRecord<?> record;

    public String getIndex() {
      return index;
    }

    public String getId() {
      return id;
    }

    public Record<?> getRecord() {
      return record;
    }

    @Override
    public int hashCode() {
      return Objects.hash(getIndex(), getId());
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Document document = (Document) o;
      return Objects.equals(getIndex(), document.getIndex())
          && Objects.equals(getId(), document.getId());
    }

    @Override
    public String toString() {
      return "Document{"
          + "index='"
          + index
          + '\''
          + ", id='"
          + id
          + '\''
          + ", record="
          + record
          + '}';
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  private static final class DocumentsWrapper {
    @JsonProperty(value = "hits", required = true)
    private List<Document> documents;
  }
}
