/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.nwe;

import io.zeebe.engine.nwe.behavior.BpmnBehaviors;
import io.zeebe.engine.nwe.container.CallActivityProcessor;
import io.zeebe.engine.nwe.container.MultiInstanceBodyProcessor;
import io.zeebe.engine.nwe.container.ProcessProcessor;
import io.zeebe.engine.nwe.container.SubProcessProcessor;
import io.zeebe.engine.nwe.event.EndEventProcessor;
import io.zeebe.engine.nwe.event.IntermediateCatchEventProcessor;
import io.zeebe.engine.nwe.event.StartEventProcessor;
import io.zeebe.engine.nwe.gateway.ExclusiveGatewayProcessor;
import io.zeebe.engine.nwe.gateway.ParallelGatewayProcessor;
import io.zeebe.engine.nwe.sequenceflow.SequenceFlowProcessor;
import io.zeebe.engine.nwe.task.ReceiveTaskProcessor;
import io.zeebe.engine.nwe.task.ServiceTaskProcessor;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableFlowElement;
import io.zeebe.protocol.record.value.BpmnElementType;
import java.util.EnumMap;
import java.util.Map;

public final class BpmnElementProcessors {

  private final Map<BpmnElementType, BpmnElementProcessor<?>> processors =
      new EnumMap<>(BpmnElementType.class);

  public BpmnElementProcessors(final BpmnBehaviors bpmnBehaviors) {
    // tasks
    processors.put(BpmnElementType.SERVICE_TASK, new ServiceTaskProcessor(bpmnBehaviors));
    processors.put(BpmnElementType.RECEIVE_TASK, new ReceiveTaskProcessor(bpmnBehaviors));
    // gateways
    processors.put(BpmnElementType.EXCLUSIVE_GATEWAY, new ExclusiveGatewayProcessor(bpmnBehaviors));
    processors.put(BpmnElementType.PARALLEL_GATEWAY, new ParallelGatewayProcessor(bpmnBehaviors));
    // containers
    processors.put(BpmnElementType.PROCESS, new ProcessProcessor(bpmnBehaviors));
    processors.put(BpmnElementType.SUB_PROCESS, new SubProcessProcessor(bpmnBehaviors));
    processors.put(
        BpmnElementType.MULTI_INSTANCE_BODY, new MultiInstanceBodyProcessor(bpmnBehaviors));
    processors.put(BpmnElementType.CALL_ACTIVITY, new CallActivityProcessor(bpmnBehaviors));
    // events
    processors.put(BpmnElementType.START_EVENT, new StartEventProcessor(bpmnBehaviors));
    processors.put(
        BpmnElementType.INTERMEDIATE_CATCH_EVENT,
        new IntermediateCatchEventProcessor(bpmnBehaviors));
    processors.put(BpmnElementType.END_EVENT, new EndEventProcessor(bpmnBehaviors));
    // others
    processors.put(BpmnElementType.SEQUENCE_FLOW, new SequenceFlowProcessor(bpmnBehaviors));
  }

  public <T extends ExecutableFlowElement> BpmnElementProcessor<T> getProcessor(
      final BpmnElementType bpmnElementType) {

    final var processor = (BpmnElementProcessor<T>) processors.get(bpmnElementType);
    if (processor == null) {
      //      throw new UnsupportedOperationException(
      //          String.format("no processor found for BPMN element type '%s'", bpmnElementType));
    }
    return processor;
  }

  public <T extends ExecutableFlowElement> BpmnElementContainerProcessor<T> getContainerProcessor(
      final BpmnElementType bpmnElementType) {
    final var processor = processors.get(bpmnElementType);
    if (processor instanceof BpmnElementContainerProcessor) {
      return (BpmnElementContainerProcessor<T>) processor;
    }
    throw new UnsupportedOperationException(
        String.format("no container processor found for BPMN element type '%s'", bpmnElementType));
  }
}
