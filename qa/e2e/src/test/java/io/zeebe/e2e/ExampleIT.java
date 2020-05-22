/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.system.configuration.ClusterCfg;
import io.zeebe.e2e.util.StreamableClusterRule;
import io.zeebe.e2e.util.containers.DockerClusterRule;
import io.zeebe.e2e.util.containers.DockerElasticStreamableClusterRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.protocol.Protocol;
import io.zeebe.protocol.record.Assertions;
import io.zeebe.protocol.record.RecordType;
import io.zeebe.protocol.record.ValueType;
import io.zeebe.protocol.record.intent.DeploymentIntent;
import io.zeebe.protocol.record.value.deployment.ResourceType;
import io.zeebe.util.VersionUtil;
import org.junit.ClassRule;
import org.junit.Test;

public final class ExampleIT {
  @ClassRule
  public static final StreamableClusterRule CLUSTER =
      new DockerElasticStreamableClusterRule(
          new DockerClusterRule(VersionUtil.getPreviousVersion(), getClusterConfig()));

  //  @ClassRule
  //  public static final StreamableClusterRule CLUSTER =
  //      new DockerDebugHttpStreambleClusterRule(
  //          new DockerClusterRule("current-test", getClusterConfig()));

  private static ClusterCfg getClusterConfig() {
    final var config = new ClusterCfg();
    config.setClusterSize(3);
    config.setReplicationFactor(3);
    config.setPartitionsCount(3);
    config.setClusterName("zeebe-cluster");

    return config;
  }

  @Test
  public void shouldDeployWorkflow() {
    // given
    final var workflow =
        Bpmn.createExecutableProcess("process").startEvent("start").endEvent("end").done();
    final var client = CLUSTER.getClient();
    final var repository = CLUSTER.getRecordRepository();

    // when
    client.newDeployCommand().addWorkflowModel(workflow, "process.bpmn").send().join();

    // then
    final var deploymentRecord = repository.deploymentRecords().limit(1).findFirst().orElseThrow();
    final var deployment = deploymentRecord.getValue();
    Assertions.assertThat(deploymentRecord)
        .hasValueType(ValueType.DEPLOYMENT)
        .hasIntent(DeploymentIntent.CREATE)
        .hasRecordType(RecordType.COMMAND)
        .hasPartitionId(Protocol.DEPLOYMENT_PARTITION);
    Assertions.assertThat(deployment).hasNoDeployedWorkflows();
    assertThat(deployment.getResources()).hasSize(1);
    Assertions.assertThat(deployment.getResources().get(0))
        .hasResourceName("process.bpmn")
        .hasResourceType(ResourceType.BPMN_XML)
        .hasResource(Bpmn.convertToString(workflow).getBytes());
  }
}
