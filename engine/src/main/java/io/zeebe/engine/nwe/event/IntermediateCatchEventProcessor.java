/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.nwe.event;

import io.zeebe.engine.nwe.BpmnElementContext;
import io.zeebe.engine.nwe.BpmnElementProcessor;
import io.zeebe.engine.nwe.behavior.BpmnBehaviors;
import io.zeebe.engine.nwe.behavior.BpmnEventSubscriptionBehavior;
import io.zeebe.engine.nwe.behavior.BpmnIncidentBehavior;
import io.zeebe.engine.nwe.behavior.BpmnStateBehavior;
import io.zeebe.engine.nwe.behavior.BpmnStateTransitionBehavior;
import io.zeebe.engine.nwe.behavior.BpmnVariableMappingBehavior;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableCatchEventElement;

public class IntermediateCatchEventProcessor
    implements BpmnElementProcessor<ExecutableCatchEventElement> {

  private final BpmnEventSubscriptionBehavior eventSubscriptionBehavior;
  private final BpmnIncidentBehavior incidentBehavior;
  private final BpmnStateBehavior stateBehavior;
  private final BpmnStateTransitionBehavior stateTransitionBehavior;
  private final BpmnVariableMappingBehavior variableMappingBehavior;

  public IntermediateCatchEventProcessor(final BpmnBehaviors bpmnBehaviors) {
    eventSubscriptionBehavior = bpmnBehaviors.eventSubscriptionBehavior();
    incidentBehavior = bpmnBehaviors.incidentBehavior();
    stateBehavior = bpmnBehaviors.stateBehavior();
    stateTransitionBehavior = bpmnBehaviors.stateTransitionBehavior();
    variableMappingBehavior = bpmnBehaviors.variableMappingBehavior();
  }

  @Override
  public Class<ExecutableCatchEventElement> getType() {
    return ExecutableCatchEventElement.class;
  }

  @Override
  public void onActivating(
      final ExecutableCatchEventElement element, final BpmnElementContext context) {
    variableMappingBehavior
        .applyInputMappings(context, element)
        .flatMap(ok -> eventSubscriptionBehavior.subscribeToEvents(element, context))
        .ifRightOrLeft(
            ok -> stateTransitionBehavior.transitionToActivated(context),
            failure -> incidentBehavior.createIncident(failure, context));
  }

  @Override
  public void onActivated(
      final ExecutableCatchEventElement element, final BpmnElementContext context) {
    // nothing to do here
  }

  @Override
  public void onCompleting(
      final ExecutableCatchEventElement element, final BpmnElementContext context) {
    variableMappingBehavior
        .applyOutputMappings(context, element)
        .ifRightOrLeft(
            ok -> {
              eventSubscriptionBehavior.unsubscribeFromEvents(context);
              stateTransitionBehavior.transitionToCompleted(context);
            },
            failure -> incidentBehavior.createIncident(failure, context));
  }

  @Override
  public void onCompleted(
      final ExecutableCatchEventElement element, final BpmnElementContext context) {
    stateTransitionBehavior.takeOutgoingSequenceFlows(element, context);
    stateBehavior.consumeToken(context);
    stateBehavior.removeElementInstance(context);
  }

  @Override
  public void onTerminating(
      final ExecutableCatchEventElement element, final BpmnElementContext context) {
    eventSubscriptionBehavior.unsubscribeFromEvents(context);
    stateTransitionBehavior.transitionToTerminated(context);
  }

  @Override
  public void onTerminated(
      final ExecutableCatchEventElement element, final BpmnElementContext context) {
    incidentBehavior.resolveIncidents(context);

    stateTransitionBehavior.onElementTerminated(element, context);

    stateBehavior.consumeToken(context);
    stateBehavior.removeElementInstance(context);
  }

  @Override
  public void onEventOccurred(
      final ExecutableCatchEventElement element, final BpmnElementContext context) {
    eventSubscriptionBehavior.triggerIntermediateEvent(context);
  }
}
