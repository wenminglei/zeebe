package io.zeebe.e2e.util.containers;

import io.zeebe.e2e.util.containers.hazelcast.HazelcastRingBufferClient;
import io.zeebe.e2e.util.record.RecordRepository;
import io.zeebe.protocol.record.Record;

public final class RecordRepositoryHazelcastAdapter implements HazelcastRingBufferClient.Listener {
  private final RecordRepository repository;

  public RecordRepositoryHazelcastAdapter(final RecordRepository repository) {
    this.repository = repository;
  }

  @Override
  public void onRecord(final Record<?> record) {
    repository.add(record);
  }

  @Override
  public void onClose() {
    repository.reset();
  }
}
