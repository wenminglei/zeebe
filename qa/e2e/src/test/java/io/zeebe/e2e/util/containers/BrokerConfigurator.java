package io.zeebe.e2e.util.containers;

import io.zeebe.containers.ZeebeBrokerContainer;

@FunctionalInterface
public interface BrokerConfigurator {
  ZeebeBrokerContainer configure(final ZeebeBrokerContainer brokerContainer);

  static BrokerConfigurator identity() {
    return b -> b;
  }
}
