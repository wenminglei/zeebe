/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.e2e.util.containers;

import io.zeebe.e2e.util.record.RecordRepository;
import io.zeebe.protocol.record.Record;

public final class RecordRepositoryExporterClientListener implements ExporterClientListener {
  private final RecordRepository repository;

  public RecordRepositoryExporterClientListener(final RecordRepository repository) {
    this.repository = repository;
  }

  @Override
  public void onExporterClientRecord(final Record<?> record) {
    repository.add(record);
  }

  @Override
  public void onExporterClientClose() {
    repository.reset();
  }
}
