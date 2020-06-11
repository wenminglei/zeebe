/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.logstreams.impl.log;

import io.atomix.raft.zeebe.ZeebeEntry;
import io.zeebe.logstreams.spi.LogStorage.AppendListener;
import java.util.NoSuchElementException;
import org.agrona.concurrent.UnsafeBuffer;

public final class Listener implements AppendListener {

  private final LogStorageAppender appender;
  private final long highestPosition;
  private final UnsafeBuffer reader;

  public Listener(
      final LogStorageAppender appender, final long highestPosition, final UnsafeBuffer reader) {
    this.appender = appender;
    this.highestPosition = highestPosition;
    this.reader = reader;
  }

  @Override
  public void onWrite(final long address) {}

  @Override
  public void onWriteError(final Throwable error) {
    LogStorageAppender.LOG.error(
        "Failed to append block with last event position {}.", highestPosition, error);
    if (error instanceof NoSuchElementException) {
      // Not a failure. It is probably during transition to follower.
      return;
    }

    appender.runOnFailure(error);
  }

  @Override
  public void onCommit(final long address) {
    releaseBackPressure();
  }

  @Override
  public void onCommitError(final long address, final Throwable error) {
    LogStorageAppender.LOG.error(
        "Failed to commit block with last event position {}.", highestPosition, error);
    releaseBackPressure();
    appender.runOnFailure(error);
  }

  @Override
  public void validatePositions(long lastPosition, final ZeebeEntry entry) {
    reader.wrap(entry.data());
    int offset = 0;

    do {
      final long position = LogEntryDescriptor.getPosition(reader, offset);
      if (lastPosition != -1 && position != lastPosition + 1) {
        throw new IllegalStateException(
            String.format(
                "Unexpected position %d was encountered after position %d when appending positions <%d, %d>.",
                position, lastPosition, entry.lowestPosition(), entry.highestPosition()));
      }
      lastPosition = position;

      offset += LogEntryDescriptor.getFragmentLength(reader, offset);
    } while (offset < reader.capacity());
  }

  private void releaseBackPressure() {
    appender.releaseBackPressure(highestPosition);
  }
}
