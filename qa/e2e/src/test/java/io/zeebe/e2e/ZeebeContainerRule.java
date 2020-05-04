package io.zeebe.e2e;

import io.zeebe.containers.ZeebeBrokerContainer;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.MountableFile;

public final class ZeebeContainerRule extends ExternalResource {
  private final Network network = Network.newNetwork();

  private ZeebeBrokerContainer zeebe = defaultZeebeContainer();
  private GenericContainer hazelcast = defaultHazelcastContainer();

  @Override
  public Statement apply(final Statement base, final Description description) {
    return super.apply(zeebe.apply(hazelcast.apply(base, description), description), description);
  }

  @Override
  protected void before() throws Throwable {
    super.before();
  }

  @Override
  protected void after() {
    super.after();
  }

  private ZeebeBrokerContainer defaultZeebeContainer() {
    return new ZeebeBrokerContainer("camunda/zeebe:0.23.1")
        .withNetwork(network)
        .withNetworkAliases("zeebe")
        .withEnv(
            "zeebe.broker.exporters.hazelcast.className",
            "io.zeebe.hazelcast.exporter.HazelcastExporter")
        .withEnv(
            "zeebe.broker.exporters.hazelcast.jarPath", "/usr/local/zeebe/exporters/hazelcast.jar")
        .withCopyFileToContainer(
            MountableFile.forClasspathResource(""), "/usr/local/zeebe/exporters/hazelcast.jar")
        .withEnv("zeebe.broker.exporters.hazelcast.remoteAddress", "hazelcast:5701")
        .withEnv("zeebe.broker.exporters.hazelcast.format", "json");
  }

  private GenericContainer defaultHazelcastContainer() {
    return new GenericContainer("hazelcast/hazelcast:3.12.6")
        .withNetwork(network)
        .withNetworkAliases("hazelcast")
        .withExposedPorts(5701)
        .withEnv("JAVA_OPTS", "-Dhazelcast.local.publicAddress=hazelcast:5701");
  }
}
