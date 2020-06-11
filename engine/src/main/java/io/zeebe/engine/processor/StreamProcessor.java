/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor;

import io.zeebe.db.DbContext;
import io.zeebe.db.ZeebeDb;
import io.zeebe.engine.state.ZeebeState;
import io.zeebe.logstreams.impl.Loggers;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamBatchWriter;
import io.zeebe.logstreams.log.LogStreamReader;
import io.zeebe.util.LangUtil;
import io.zeebe.util.health.FailureListener;
import io.zeebe.util.health.HealthMonitorable;
import io.zeebe.util.health.HealthStatus;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.ActorCondition;
import io.zeebe.util.sched.ActorScheduler;
import io.zeebe.util.sched.clock.ActorClock;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;

public class StreamProcessor extends Actor implements HealthMonitorable {
  static final Duration HEALTH_CHECK_TICK_DURATION = Duration.ofSeconds(5);
  static final long UNSET_POSITION = -1L;
  private static final String ERROR_MESSAGE_RECOVER_FROM_SNAPSHOT_FAILED =
      "Expected to find event with the snapshot position %s in log stream, but nothing was found. Failed to recover '%s'.";
  private static final Logger LOG = Loggers.LOGSTREAMS_LOGGER;
  private final ActorScheduler actorScheduler;
  private final AtomicBoolean isOpened = new AtomicBoolean(false);
  private final List<StreamProcessorLifecycleAware> lifecycleAwareListeners;

  // log stream
  private final LogStream logStream;
  private final int partitionId;
  // snapshotting
  private final ZeebeDb zeebeDb;
  // processing
  private final ProcessingContext processingContext;
  private final TypedRecordProcessorFactory typedRecordProcessorFactory;
  private LogStreamReader logStreamReader;
  private ActorCondition onCommitPositionUpdatedCondition;
  private long snapshotPosition = -1L;
  private ProcessingStateMachine processingStateMachine;

  private Phase phase = Phase.REPROCESSING;
  private CompletableActorFuture<Void> openFuture;
  private CompletableActorFuture<Void> closeFuture = CompletableActorFuture.completed(null);
  private final String actorName;
  private FailureListener failureListener;
  private volatile long lastTickTime;
  private boolean shouldProcess;

  protected StreamProcessor(final StreamProcessorBuilder processorBuilder) {
    this.actorScheduler = processorBuilder.getActorScheduler();
    this.lifecycleAwareListeners = processorBuilder.getLifecycleListeners();

    this.typedRecordProcessorFactory = processorBuilder.getTypedRecordProcessorFactory();
    this.zeebeDb = processorBuilder.getZeebeDb();

    processingContext =
        processorBuilder
            .getProcessingContext()
            .eventCache(new RecordValues())
            .actor(actor)
            .abortCondition(this::isClosed);
    this.logStream = processingContext.getLogStream();
    this.partitionId = logStream.getPartitionId();
    this.actorName = buildActorName(processorBuilder.getNodeId(), "StreamProcessor-" + partitionId);
  }

  public static StreamProcessorBuilder builder() {
    return new StreamProcessorBuilder();
  }

  @Override
  public String getName() {
    return actorName;
  }

  @Override
  protected void onActorStarting() {
    actor.runOnCompletionBlockingCurrentPhase(
        logStream.newLogStreamBatchWriter(), this::onRetrievingWriter);
  }

  @Override
  protected void onActorStarted() {
    try {
      LOG.debug("Recovering state of partition {} from snapshot", partitionId);
      snapshotPosition = recoverFromSnapshot();

      initProcessors();
    } catch (final Throwable e) {
      onFailure(e);
      LangUtil.rethrowUnchecked(e);
    }

    try {
      processingStateMachine =
          new ProcessingStateMachine(processingContext, this::shouldProcessNext);
      openFuture.complete(null);

      final ReProcessingStateMachine reProcessingStateMachine =
          new ReProcessingStateMachine(processingContext);

      healthCheckTick();
      final ActorFuture<Void> recoverFuture =
          reProcessingStateMachine.startRecover(snapshotPosition);

      actor.runOnCompletion(
          recoverFuture,
          (v, throwable) -> {
            if (throwable != null) {
              LOG.error("Unexpected error on recovery happens.", throwable);
              onFailure(throwable);
            } else {
              onRecovered();
            }
          });
    } catch (final RuntimeException e) {
      onFailure(e);
      throw e;
    }
  }

  @Override
  protected void onActorClosing() {
    tearDown();
  }

  @Override
  protected void onActorClosed() {
    closeFuture.complete(null);
    LOG.debug("Closed stream processor controller {}.", getName());
  }

  @Override
  protected void onActorCloseRequested() {
    if (!isFailed()) {
      lifecycleAwareListeners.forEach(StreamProcessorLifecycleAware::onClose);
    }
  }

  @Override
  public ActorFuture<Void> closeAsync() {
    if (isOpened.compareAndSet(true, false)) {
      closeFuture = new CompletableActorFuture<>();
      actor.close();
    }
    return closeFuture;
  }

  @Override
  protected void handleFailure(final Exception failure) {
    LOG.error("Actor {} failed in phase {}.", actorName, actor.getLifecyclePhase(), failure);
    if (this.failureListener != null) {
      this.failureListener.onFailure();
    }
    actor.fail();
  }

  @Override
  public void onActorFailed() {
    phase = Phase.FAILED;
    closeFuture = CompletableActorFuture.completed(null);
    isOpened.set(false);
    lifecycleAwareListeners.forEach(StreamProcessorLifecycleAware::onFailed);
    tearDown();
  }

  private boolean shouldProcessNext() {
    return isOpened() && shouldProcess;
  }

  private void tearDown() {
    processingContext.getLogStreamReader().close();

    if (onCommitPositionUpdatedCondition != null) {
      logStream.removeOnCommitPositionUpdatedCondition(onCommitPositionUpdatedCondition);
      onCommitPositionUpdatedCondition = null;
    }
  }

  private void healthCheckTick() {
    lastTickTime = ActorClock.currentTimeMillis();
    actor.runDelayed(HEALTH_CHECK_TICK_DURATION, this::healthCheckTick);
  }

  private void onRetrievingWriter(
      final LogStreamBatchWriter batchWriter, final Throwable errorOnReceivingWriter) {

    if (errorOnReceivingWriter == null) {
      processingContext
          .maxFragmentSize(batchWriter.getMaxFragmentLength())
          .logStreamWriter(new TypedStreamWriterImpl(batchWriter));

      actor.runOnCompletionBlockingCurrentPhase(
          logStream.newLogStreamReader(), this::onRetrievingReader);
    } else {
      LOG.error(
          "Unexpected error on retrieving batch writer from log stream.", errorOnReceivingWriter);
      actor.close();
    }
  }

  private void onRetrievingReader(
      final LogStreamReader reader, final Throwable errorOnReceivingReader) {
    if (errorOnReceivingReader == null) {
      this.logStreamReader = reader;
      processingContext.logStreamReader(reader);
    } else {
      LOG.error("Unexpected error on retrieving reader from log stream.", errorOnReceivingReader);
      actor.close();
    }
  }

  public ActorFuture<Void> openAsync() {
    if (isOpened.compareAndSet(false, true)) {
      openFuture = new CompletableActorFuture<>();
      actorScheduler.submitActor(this);
    }
    return openFuture;
  }

  private void initProcessors() {
    final TypedRecordProcessors typedRecordProcessors =
        typedRecordProcessorFactory.createProcessors(processingContext);

    lifecycleAwareListeners.addAll(typedRecordProcessors.getLifecycleListeners());
    final RecordProcessorMap recordProcessorMap = typedRecordProcessors.getRecordProcessorMap();
    recordProcessorMap.values().forEachRemaining(this.lifecycleAwareListeners::add);

    processingContext.recordProcessorMap(recordProcessorMap);
  }

  private long recoverFromSnapshot() {
    final ZeebeState zeebeState = recoverState();
    final long snapshotPosition = zeebeState.getLastSuccessfulProcessedRecordPosition();

    final boolean failedToRecoverReader = !logStreamReader.seekToNextEvent(snapshotPosition);
    if (failedToRecoverReader) {
      throw new IllegalStateException(
          String.format(ERROR_MESSAGE_RECOVER_FROM_SNAPSHOT_FAILED, snapshotPosition, getName()));
    }

    LOG.info(
        "Recovered state of partition {} from snapshot at position {}",
        partitionId,
        snapshotPosition);
    return snapshotPosition;
  }

  private ZeebeState recoverState() {
    final DbContext dbContext = zeebeDb.createContext();
    final ZeebeState zeebeState = new ZeebeState(partitionId, zeebeDb, dbContext);

    processingContext.dbContext(dbContext);
    processingContext.zeebeState(zeebeState);

    return zeebeState;
  }

  private void onRecovered() {
    phase = Phase.PROCESSING;
    onCommitPositionUpdatedCondition =
        actor.onCondition(
            getName() + "-on-commit-position-updated", processingStateMachine::readNextEvent);
    logStream.registerOnCommitPositionUpdatedCondition(onCommitPositionUpdatedCondition);

    // start reading
    lifecycleAwareListeners.forEach(l -> l.onRecovered(processingContext));
    actor.submit(processingStateMachine::readNextEvent);
  }

  private void onFailure(final Throwable throwable) {
    actor.fail();
    if (!openFuture.isDone()) {
      openFuture.completeExceptionally(throwable);
    }
    if (failureListener != null) {
      failureListener.onFailure();
    }
  }

  public boolean isOpened() {
    return isOpened.get();
  }

  public boolean isClosed() {
    return !isOpened.get();
  }

  public boolean isFailed() {
    return phase == Phase.FAILED;
  }

  public ActorFuture<Long> getLastProcessedPositionAsync() {
    return actor.call(processingStateMachine::getLastSuccessfulProcessedEventPosition);
  }

  public ActorFuture<Long> getLastWrittenPositionAsync() {
    return actor.call(processingStateMachine::getLastWrittenEventPosition);
  }

  @Override
  public HealthStatus getHealthStatus() {
    if (actor.isClosed()) {
      return HealthStatus.UNHEALTHY;
    }

    if (!processingStateMachine.isMakingProgress()) {
      return HealthStatus.UNHEALTHY;
    }

    // If healthCheckTick was not invoked it indicates the actor is blocked in a runUntilDone loop.
    if (ActorClock.currentTimeMillis() - lastTickTime > HEALTH_CHECK_TICK_DURATION.toMillis() * 2) {
      return HealthStatus.UNHEALTHY;
    } else {
      return HealthStatus.HEALTHY;
    }
  }

  @Override
  public void addFailureListener(final FailureListener failureListener) {
    actor.run(() -> this.failureListener = failureListener);
  }

  public void pauseProcessing() {
    actor.call(
        () -> {
          if (this.shouldProcess) {
            lifecycleAwareListeners.forEach(StreamProcessorLifecycleAware::onPaused);
            this.shouldProcess = false;
            phase = Phase.PROCESSING;
          }
        });
  }

  public void resumeProcessing() {
    actor.call(
        () -> {
          if (!this.shouldProcess) {
            lifecycleAwareListeners.forEach(StreamProcessorLifecycleAware::onResumed);
            this.shouldProcess = true;
            phase = Phase.PAUSED;
          }
        });
  }

  private enum Phase {
    REPROCESSING,
    PROCESSING,
    FAILED,
    PAUSED,
  }
}
