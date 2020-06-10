/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.nwe.container;

import io.zeebe.engine.nwe.BpmnElementContainerProcessor;
import io.zeebe.engine.nwe.BpmnElementContext;
import io.zeebe.engine.nwe.BpmnProcessingException;
import io.zeebe.engine.nwe.behavior.BpmnBehaviors;
import io.zeebe.engine.nwe.behavior.BpmnEventSubscriptionBehavior;
import io.zeebe.engine.nwe.behavior.BpmnIncidentBehavior;
import io.zeebe.engine.nwe.behavior.BpmnStateBehavior;
import io.zeebe.engine.nwe.behavior.BpmnStateTransitionBehavior;
import io.zeebe.engine.nwe.behavior.BpmnVariableMappingBehavior;
import io.zeebe.engine.processor.Failure;
import io.zeebe.engine.processor.workflow.ExpressionProcessor;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableCallActivity;
import io.zeebe.engine.state.deployment.DeployedWorkflow;
import io.zeebe.protocol.record.value.ErrorType;
import io.zeebe.util.Either;
import io.zeebe.util.buffer.BufferUtil;

public class CallActivityProcessor
    implements BpmnElementContainerProcessor<ExecutableCallActivity> {

  private final ExpressionProcessor expressionProcessor;
  private final BpmnStateTransitionBehavior stateTransitionBehavior;
  private final BpmnStateBehavior stateBehavior;
  private final BpmnIncidentBehavior incidentBehavior;
  private final BpmnEventSubscriptionBehavior eventSubscriptionBehavior;
  private final BpmnVariableMappingBehavior variableMappingBehavior;

  public CallActivityProcessor(final BpmnBehaviors bpmnBehaviors) {
    expressionProcessor = bpmnBehaviors.expressionBehavior();
    stateTransitionBehavior = bpmnBehaviors.stateTransitionBehavior();
    stateBehavior = bpmnBehaviors.stateBehavior();
    incidentBehavior = bpmnBehaviors.incidentBehavior();
    eventSubscriptionBehavior = bpmnBehaviors.eventSubscriptionBehavior();
    variableMappingBehavior = bpmnBehaviors.variableMappingBehavior();
  }

  @Override
  public Class<ExecutableCallActivity> getType() {
    return ExecutableCallActivity.class;
  }

  @Override
  public void onActivating(final ExecutableCallActivity element, final BpmnElementContext context) {
    variableMappingBehavior
        .applyInputMappings(context, element)
        .flatMap(ok -> eventSubscriptionBehavior.subscribeToEvents(element, context))
        .flatMap(ok -> evaluateProcessId(context, element))
        .flatMap(this::getWorkflowForProcessId)
        .flatMap(this::checkWorkflowHasNoneStartEvent)
        .ifRightOrLeft(
            workflow -> {
              final var childWorkflowInstanceKey =
                  stateTransitionBehavior.createChildProcessInstance(workflow, context);

              final var callActivityInstance = stateBehavior.getElementInstance(context);
              callActivityInstance.setCalledChildInstanceKey(childWorkflowInstanceKey);
              stateBehavior.updateElementInstance(callActivityInstance);

              final var callActivityInstanceKey = context.getElementInstanceKey();
              stateBehavior.copyVariables(
                  callActivityInstanceKey, childWorkflowInstanceKey, workflow);

              stateTransitionBehavior.transitionToActivated(context);
            },
            failure -> incidentBehavior.createIncident(failure, context));
  }

  @Override
  public void onActivated(final ExecutableCallActivity element, final BpmnElementContext context) {
    throw new BpmnProcessingException(context, "Not yet implemented");
  }

  @Override
  public void onCompleting(final ExecutableCallActivity element, final BpmnElementContext context) {
    throw new BpmnProcessingException(context, "Not yet implemented");
  }

  @Override
  public void onCompleted(final ExecutableCallActivity element, final BpmnElementContext context) {
    throw new BpmnProcessingException(context, "Not yet implemented");
  }

  @Override
  public void onTerminating(
      final ExecutableCallActivity element, final BpmnElementContext context) {
    throw new BpmnProcessingException(context, "Not yet implemented");
  }

  @Override
  public void onTerminated(final ExecutableCallActivity element, final BpmnElementContext context) {
    throw new BpmnProcessingException(context, "Not yet implemented");
  }

  @Override
  public void onEventOccurred(
      final ExecutableCallActivity element, final BpmnElementContext context) {
    throw new BpmnProcessingException(context, "Not yet implemented");
  }

  @Override
  public void onChildCompleted(
      final ExecutableCallActivity element,
      final BpmnElementContext flowScopeContext,
      final BpmnElementContext childContext) {
    throw new BpmnProcessingException(flowScopeContext, "Not yet implemented");
  }

  @Override
  public void onChildTerminated(
      final ExecutableCallActivity element,
      final BpmnElementContext flowScopeContext,
      final BpmnElementContext childContext) {
    throw new BpmnProcessingException(flowScopeContext, "Not yet implemented");
  }

  private Either<Failure, String> evaluateProcessId(
      final BpmnElementContext context, final ExecutableCallActivity element) {
    final var processIdExpression = element.getCalledElementProcessId();
    final var scopeKey = context.getElementInstanceKey();
    return expressionProcessor.evaluateStringExpression(processIdExpression, scopeKey);
  }

  @SuppressWarnings("OptionalIsPresent") // the proposed refactoring is less readable
  private Either<Failure, DeployedWorkflow> getWorkflowForProcessId(final String processId) {
    final var workflow = stateBehavior.getWorkflow(processId);
    if (workflow.isPresent()) {
      return Either.right(workflow.get());
    }
    return Either.left(
        new Failure(
            String.format(
                "Expected workflow with BPMN process id '%s' to be deployed, but not found.",
                processId),
            ErrorType.CALLED_ELEMENT_ERROR));
  }

  private Either<Failure, DeployedWorkflow> checkWorkflowHasNoneStartEvent(
      final DeployedWorkflow workflow) {
    if (workflow.getWorkflow().getNoneStartEvent() == null) {
      return Either.left(
          new Failure(
              String.format(
                  "Expected workflow with BPMN process id '%s' to have a none start event, but not found.",
                  BufferUtil.bufferAsString(workflow.getBpmnProcessId())),
              ErrorType.CALLED_ELEMENT_ERROR));
    }
    return Either.right(workflow);
  }
}
