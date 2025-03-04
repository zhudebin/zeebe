/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.streamprocessor;

import io.camunda.zeebe.db.TransactionContext;
import io.camunda.zeebe.db.ZeebeDb;
import io.camunda.zeebe.engine.Engine;
import io.camunda.zeebe.engine.EngineContext;
import io.camunda.zeebe.engine.api.StreamProcessorLifecycleAware;
import io.camunda.zeebe.engine.metrics.StreamProcessorMetrics;
import io.camunda.zeebe.engine.processing.streamprocessor.RecordValues;
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecordProcessorFactory;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.LegacyTypedStreamWriter;
import io.camunda.zeebe.engine.state.EventApplier;
import io.camunda.zeebe.engine.state.ZeebeDbState;
import io.camunda.zeebe.engine.state.mutable.MutableZeebeState;
import io.camunda.zeebe.logstreams.impl.Loggers;
import io.camunda.zeebe.logstreams.log.LogRecordAwaiter;
import io.camunda.zeebe.logstreams.log.LogStream;
import io.camunda.zeebe.logstreams.log.LogStreamBatchWriter;
import io.camunda.zeebe.logstreams.log.LogStreamReader;
import io.camunda.zeebe.scheduler.Actor;
import io.camunda.zeebe.scheduler.ActorSchedulingService;
import io.camunda.zeebe.scheduler.clock.ActorClock;
import io.camunda.zeebe.scheduler.future.ActorFuture;
import io.camunda.zeebe.scheduler.future.CompletableActorFuture;
import io.camunda.zeebe.streamprocessor.state.StreamProcessorDbState;
import io.camunda.zeebe.util.exception.UnrecoverableException;
import io.camunda.zeebe.util.health.FailureListener;
import io.camunda.zeebe.util.health.HealthMonitorable;
import io.camunda.zeebe.util.health.HealthReport;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.slf4j.Logger;

/*

+-------------------+
|                   |
|   ActorStarting   |
|                   |
+-------------------+
          |
          v
+-------------------+
|                   |                                    +-----------------+
|   Create Reader   |                                    |                 |
|                   |       ---------------------------> |    Actor close  | <-------------------------------
+-------------------+       |          |                 |                 |                                 |
          |                 |          |                 +-----------------+                                 |
          v                 |          |                                                                     |
+-------------------+       |          |                                                                     |
|                   |       |    +-----------+        +-------------+        +-----------------+      +------------+
|   Actor Started   |--------    |           |        |             |        |                 |      |            |
|                   |----------->|   Replay  |------->|   Replay    |------->| Create writer   | ---->|   Process  |
+-------------------+            |           |        |   Completed |        |                 |      |            |
                                 +-----------+        +-------------+        +-----------------+      +------------+
                                        |                                            |                      |
                                        |                                            |                      |
                                        |                                            |                      |
                                        v                                            |                      |
                                  +-------------+                                    |                      |
                                  |   Actor     |                                    |                      |
                                  |   Failed    |  <---------------------------------------------------------
                                  |             |
                                  +-------------+


https://textik.com/#f8692d3c3e76c699
*/
public class StreamProcessor extends Actor implements HealthMonitorable, LogRecordAwaiter {

  public static final long UNSET_POSITION = -1L;
  public static final Duration HEALTH_CHECK_TICK_DURATION = Duration.ofSeconds(5);

  private static final String ERROR_MESSAGE_RECOVER_FROM_SNAPSHOT_FAILED =
      "Expected to find event with the snapshot position %s in log stream, but nothing was found. Failed to recover '%s'.";
  private static final Logger LOG = Loggers.LOGSTREAMS_LOGGER;
  private final ActorSchedulingService actorSchedulingService;
  private final AtomicBoolean isOpened = new AtomicBoolean(false);
  private final List<StreamProcessorLifecycleAware> lifecycleAwareListeners;
  private final Function<MutableZeebeState, EventApplier> eventApplierFactory;
  private final Set<FailureListener> failureListeners = new HashSet<>();
  private final StreamProcessorMetrics metrics;

  // log stream
  private final LogStream logStream;
  private final int partitionId;
  // snapshotting
  private final ZeebeDb zeebeDb;
  // processing
  private final StreamProcessorContext streamProcessorContext;
  private final TypedRecordProcessorFactory typedRecordProcessorFactory;
  private final String actorName;
  private LogStreamReader logStreamReader;
  private ProcessingStateMachine processingStateMachine;
  private ReplayStateMachine replayStateMachine;

  private volatile Phase phase = Phase.INITIAL;

  private CompletableActorFuture<Void> openFuture;
  private final CompletableActorFuture<Void> closeFuture = new CompletableActorFuture<>();
  private volatile long lastTickTime;
  private boolean shouldProcess = true;
  private ActorFuture<LastProcessingPositions> replayCompletedFuture;
  private final Function<LogStreamBatchWriter, LegacyTypedStreamWriter> typedStreamWriterFactory;

  private final Engine engine;
  private StreamProcessorDbState streamProcessorDbState;

  protected StreamProcessor(final StreamProcessorBuilder processorBuilder) {
    actorSchedulingService = processorBuilder.getActorSchedulingService();
    lifecycleAwareListeners = processorBuilder.getLifecycleListeners();

    typedRecordProcessorFactory = processorBuilder.getTypedRecordProcessorFactory();
    typedStreamWriterFactory = processorBuilder.getTypedStreamWriterFactory();
    zeebeDb = processorBuilder.getZeebeDb();

    eventApplierFactory = processorBuilder.getEventApplierFactory();

    streamProcessorContext =
        processorBuilder
            .getProcessingContext()
            .eventCache(new RecordValues())
            .actor(actor)
            .abortCondition(this::isClosed);
    logStream = streamProcessorContext.getLogStream();
    partitionId = logStream.getPartitionId();
    actorName = buildActorName(processorBuilder.getNodeId(), "StreamProcessor", partitionId);
    metrics = new StreamProcessorMetrics(partitionId);

    engine = new Engine();
  }

  public static StreamProcessorBuilder builder() {
    return new StreamProcessorBuilder();
  }

  @Deprecated // only used for tests
  public StreamProcessorDbState getStreamProcessorDbState() {
    return streamProcessorDbState;
  }

  @Override
  protected Map<String, String> createContext() {
    final var context = super.createContext();
    context.put(ACTOR_PROP_PARTITION_ID, Integer.toString(partitionId));
    return context;
  }

  @Override
  public String getName() {
    return actorName;
  }

  @Override
  protected void onActorStarting() {
    actor.runOnCompletionBlockingCurrentPhase(
        logStream.newLogStreamReader(), this::onRetrievingReader);
  }

  @Override
  protected void onActorStarted() {
    try {
      LOG.debug("Recovering state of partition {} from snapshot", partitionId);
      final var startRecoveryTimer = metrics.startRecoveryTimer();
      final long snapshotPosition = recoverFromSnapshot();

      initEngine();

      healthCheckTick();

      replayStateMachine =
          new ReplayStateMachine(engine, streamProcessorContext, this::shouldProcessNext);
      // disable writing to the log stream
      streamProcessorContext.disableLogStreamWriter();

      openFuture.complete(null);
      replayCompletedFuture = replayStateMachine.startRecover(snapshotPosition);

      if (!shouldProcess) {
        setStateToPausedAndNotifyListeners();
      } else {
        phase = Phase.REPLAY;
      }

      if (isInReplayOnlyMode()) {
        replayCompletedFuture.onComplete(
            (v, error) -> {
              if (error != null) {
                LOG.error("The replay of events failed.", error);
                onFailure(error);
              }
            });

      } else {
        replayCompletedFuture.onComplete(
            (lastProcessingPositions, error) -> {
              if (error != null) {
                LOG.error("The replay of events failed.", error);
                onFailure(error);
              } else {
                onRecovered(lastProcessingPositions);
                // observe recovery time
                startRecoveryTimer.close();
              }
            });
      }
    } catch (final RuntimeException e) {
      onFailure(e);
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
    isOpened.set(false);
    actor.close();
    return closeFuture;
  }

  @Override
  protected void handleFailure(final Throwable failure) {
    onFailure(failure);
  }

  @Override
  public void onActorFailed() {
    phase = Phase.FAILED;
    isOpened.set(false);
    lifecycleAwareListeners.forEach(StreamProcessorLifecycleAware::onFailed);
    tearDown();
    closeFuture.complete(null);
  }

  private boolean shouldProcessNext() {
    return isOpened() && shouldProcess;
  }

  private void tearDown() {
    streamProcessorContext.getLogStreamReader().close();
    logStream.removeRecordAvailableListener(this);
    replayStateMachine.close();
  }

  private void healthCheckTick() {
    lastTickTime = ActorClock.currentTimeMillis();
    actor.runDelayed(HEALTH_CHECK_TICK_DURATION, this::healthCheckTick);
  }

  private void onRetrievingWriter(
      final LogStreamBatchWriter batchWriter,
      final Throwable errorOnReceivingWriter,
      final LastProcessingPositions lastProcessingPositions) {

    if (errorOnReceivingWriter == null) {
      streamProcessorContext
          .maxFragmentSize(batchWriter.getMaxFragmentLength())
          .logStreamWriter(typedStreamWriterFactory.apply(batchWriter));

      streamProcessorContext.logStreamBatchWriter(batchWriter);

      phase = Phase.PROCESSING;

      // enable writing records to the stream
      streamProcessorContext.enableLogStreamWriter();

      processingStateMachine =
          new ProcessingStateMachine(streamProcessorContext, this::shouldProcessNext, engine);

      logStream.registerRecordAvailableListener(this);

      // start reading
      lifecycleAwareListeners.forEach(l -> l.onRecovered(streamProcessorContext));
      processingStateMachine.startProcessing(lastProcessingPositions);
      if (!shouldProcess) {
        setStateToPausedAndNotifyListeners();
      }
    } else {
      onFailure(errorOnReceivingWriter);
    }
  }

  private void onRetrievingReader(
      final LogStreamReader reader, final Throwable errorOnReceivingReader) {
    if (errorOnReceivingReader == null) {
      logStreamReader = reader;
      streamProcessorContext.logStreamReader(reader);
    } else {
      LOG.error("Unexpected error on retrieving reader from log stream.", errorOnReceivingReader);
      actor.close();
    }
  }

  public ActorFuture<Void> openAsync(final boolean pauseOnStart) {
    if (isOpened.compareAndSet(false, true)) {
      shouldProcess = !pauseOnStart;
      openFuture = new CompletableActorFuture<>();
      actorSchedulingService.submitActor(this);
    }
    return openFuture;
  }

  private void initEngine() {
    final var engineContext =
        new EngineContext(
            partitionId,
            streamProcessorContext.getScheduleService(),
            zeebeDb,
            streamProcessorContext.getTransactionContext(),
            streamProcessorContext.getLogStreamWriter(),
            streamProcessorContext.getTypedResponseWriter(),
            eventApplierFactory,
            typedRecordProcessorFactory);
    engine.init(engineContext);

    lifecycleAwareListeners.addAll(engineContext.getLifecycleListeners());
    final var listener = engineContext.getStreamProcessorListener();
    if (listener != null) {
      streamProcessorContext.listener(engineContext.getStreamProcessorListener());
    }
    streamProcessorContext.writers(engineContext.getWriters());
  }

  private long recoverFromSnapshot() {
    final TransactionContext transactionContext = zeebeDb.createContext();
    streamProcessorContext.transactionContext(transactionContext);
    recoverZeebeDbState(transactionContext);

    streamProcessorDbState = new StreamProcessorDbState(zeebeDb, transactionContext);
    streamProcessorContext.lastProcessedPositionState(
        getStreamProcessorDbState().getLastProcessedPositionState());

    final long snapshotPosition =
        streamProcessorDbState
            .getLastProcessedPositionState()
            .getLastSuccessfulProcessedRecordPosition();

    final boolean failedToRecoverReader = !logStreamReader.seekToNextEvent(snapshotPosition);
    if (failedToRecoverReader
        && streamProcessorContext.getProcessorMode() == StreamProcessorMode.PROCESSING) {
      throw new IllegalStateException(
          String.format(ERROR_MESSAGE_RECOVER_FROM_SNAPSHOT_FAILED, snapshotPosition, getName()));
    }

    LOG.info(
        "Recovered state of partition {} from snapshot at position {}",
        partitionId,
        snapshotPosition);
    return snapshotPosition;
  }

  // TODO move ZeebeDbStateCreation/EventApplier into engine; decide whether transaction context is
  // shared or not
  private void recoverZeebeDbState(final TransactionContext transactionContext) {
    final ZeebeDbState zeebeState = new ZeebeDbState(partitionId, zeebeDb, transactionContext);
    streamProcessorContext.zeebeState(zeebeState);
    streamProcessorContext.eventApplier(eventApplierFactory.apply(zeebeState));
  }

  private void onRecovered(final LastProcessingPositions lastProcessingPositions) {
    logStream
        .newLogStreamBatchWriter()
        .onComplete(
            (batchWriter, errorOnReceivingWriter) ->
                onRetrievingWriter(batchWriter, errorOnReceivingWriter, lastProcessingPositions));
  }

  private void onFailure(final Throwable throwable) {
    LOG.error("Actor {} failed in phase {}.", actorName, actor.getLifecyclePhase(), throwable);
    actor.fail(throwable);
    if (!openFuture.isDone()) {
      openFuture.completeExceptionally(throwable);
    }

    if (throwable instanceof UnrecoverableException) {
      final var report = HealthReport.dead(this).withIssue(throwable);
      failureListeners.forEach(l -> l.onUnrecoverableFailure(report));
    } else {
      final var report = HealthReport.unhealthy(this).withIssue(throwable);
      failureListeners.forEach(l -> l.onFailure(report));
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
    return actor.call(
        () -> {
          if (isInReplayOnlyMode()) {
            return replayStateMachine.getLastSourceEventPosition();
          } else if (processingStateMachine == null) {
            // StreamProcessor is still replay mode
            return StreamProcessor.UNSET_POSITION;
          } else {
            return processingStateMachine.getLastSuccessfulProcessedRecordPosition();
          }
        });
  }

  private boolean isInReplayOnlyMode() {
    return streamProcessorContext.getProcessorMode() == StreamProcessorMode.REPLAY;
  }

  public ActorFuture<Long> getLastWrittenPositionAsync() {
    return actor.call(
        () -> {
          if (isInReplayOnlyMode()) {
            return replayStateMachine.getLastReplayedEventPosition();
          } else if (processingStateMachine == null) {
            // StreamProcessor is still replay mode
            return StreamProcessor.UNSET_POSITION;
          } else {
            return processingStateMachine.getLastWrittenPosition();
          }
        });
  }

  @Override
  public HealthReport getHealthReport() {
    if (actor.isClosed()) {
      return HealthReport.unhealthy(this).withMessage("actor is closed");
    }

    if (processingStateMachine != null && !processingStateMachine.isMakingProgress()) {
      return HealthReport.unhealthy(this).withMessage("not making progress");
    }

    // If healthCheckTick was not invoked it indicates the actor is blocked in a runUntilDone loop.
    if (ActorClock.currentTimeMillis() - lastTickTime > HEALTH_CHECK_TICK_DURATION.toMillis() * 2) {
      return HealthReport.unhealthy(this).withMessage("actor appears blocked");
    } else if (phase == Phase.FAILED) {
      return HealthReport.unhealthy(this).withMessage("in failed phase");
    } else {
      return HealthReport.healthy(this);
    }
  }

  @Override
  public void addFailureListener(final FailureListener failureListener) {
    actor.run(() -> failureListeners.add(failureListener));
  }

  @Override
  public void removeFailureListener(final FailureListener failureListener) {
    actor.run(() -> failureListeners.remove(failureListener));
  }

  public ActorFuture<Phase> getCurrentPhase() {
    return actor.call(() -> phase);
  }

  public ActorFuture<Void> pauseProcessing() {
    return actor.call(
        () -> {
          if (shouldProcess) {
            setStateToPausedAndNotifyListeners();
          }
        });
  }

  public ActorFuture<Boolean> hasProcessingReachedTheEnd() {
    return actor.call(
        () ->
            processingStateMachine != null
                && !isInReplayOnlyMode()
                && processingStateMachine.hasReachedEnd());
  }

  private void setStateToPausedAndNotifyListeners() {
    if (isInReplayOnlyMode() || !replayCompletedFuture.isDone()) {
      LOG.debug("Paused replay for partition {}", partitionId);
    } else {
      lifecycleAwareListeners.forEach(StreamProcessorLifecycleAware::onPaused);
      LOG.debug("Paused processing for partition {}", partitionId);
    }

    shouldProcess = false;
    phase = Phase.PAUSED;
  }

  public void resumeProcessing() {
    actor.call(
        () -> {
          if (!shouldProcess) {
            shouldProcess = true;

            if (isInReplayOnlyMode() || !replayCompletedFuture.isDone()) {
              phase = Phase.REPLAY;
              actor.submit(replayStateMachine::replayNextEvent);
              LOG.debug("Resumed replay for partition {}", partitionId);
            } else {
              // we only want to call the lifecycle listeners on processing resume
              // since the listeners are not recovered yet
              lifecycleAwareListeners.forEach(StreamProcessorLifecycleAware::onResumed);
              phase = Phase.PROCESSING;
              if (processingStateMachine != null) {
                actor.submit(processingStateMachine::readNextRecord);
              }
              LOG.debug("Resumed processing for partition {}", partitionId);
            }
          }
        });
  }

  @Override
  public void onRecordAvailable() {
    actor.run(processingStateMachine::readNextRecord);
  }

  public enum Phase {
    INITIAL,
    REPLAY,
    PROCESSING,
    FAILED,
    PAUSED,
  }
}
