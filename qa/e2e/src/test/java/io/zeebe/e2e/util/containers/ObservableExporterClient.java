package io.zeebe.e2e.util.containers;

import io.zeebe.protocol.record.Record;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.helpers.NOPLogger;

public interface ObservableExporterClient extends ExporterClientListener {
  Set<ExporterClientListener> getListeners();

  default void addListener(final ExporterClientListener listener) {
    getListeners().add(listener);
  }

  default void removeListener(final ExporterClientListener listener) {
    getListeners().remove(listener);
  }

  default Logger getLogger() {
    return NOPLogger.NOP_LOGGER;
  }

  @Override
  default void onExporterClientRecord(final Record<?> record) {
    final var listeners = getListeners();
    for (final var listener : listeners) {
      try {
        listener.onExporterClientRecord(record);
      } catch (final Exception e) {
        getLogger()
            .error(
                "Unexpected error occurred while trying to notify exporter client listener {} about record {}",
                listener,
                record,
                e);
      }
    }
  }

  @Override
  default void onExporterClientClose() {
    final var listeners = getListeners();
    for (final var listener : listeners) {
      try {
        listener.onExporterClientClose();
      } catch (final Exception e) {
        getLogger()
            .error(
                "Unexpected error occurred while trying to close exporter client listener {}",
                listener,
                e);
      }
    }
  }
}
