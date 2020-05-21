package io.zeebe.e2e.util.containers;

import io.zeebe.containers.ZeebeBrokerContainer;
import io.zeebe.containers.ZeebeStandaloneGatewayContainer;
import java.util.Map;

@FunctionalInterface
public interface GatewayConfigurator {
  ZeebeStandaloneGatewayContainer configure(
      ZeebeStandaloneGatewayContainer gatewayContainer, Map<Integer, ZeebeBrokerContainer> brokers);

  static GatewayConfigurator identity() {
    return (gateway, brokers) -> gateway;
  }
}
