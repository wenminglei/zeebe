package io.zeebe.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.e2e.util.containers.ZeebeContainerRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.protocol.Protocol;
import io.zeebe.protocol.record.Assertions;
import io.zeebe.protocol.record.RecordType;
import io.zeebe.protocol.record.ValueType;
import io.zeebe.protocol.record.intent.DeploymentIntent;
import io.zeebe.protocol.record.value.deployment.ResourceType;
import org.junit.Rule;
import org.junit.Test;

public final class ExampleIT {
  @Rule public final ZeebeContainerRule containerRule = new ZeebeContainerRule();

  @Test
  public void shouldDeployWorkflow() {
    // given
    final var workflow =
        Bpmn.createExecutableProcess("process").startEvent("start").endEvent("end").done();
    final var client = containerRule.newZeebeClient();
    final var repository = containerRule.newRecordRepository();

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
