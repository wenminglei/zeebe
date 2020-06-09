/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.monitoring;

import static io.zeebe.broker.Broker.LOG;

import io.zeebe.broker.system.configuration.DataCfg;
import io.zeebe.util.ByteValue;
import io.zeebe.util.sched.Actor;
import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class DiskSpaceUsageMonitor extends Actor {

  private static final Duration DISK_USAGE_CHECK_DELAY = Duration.ofSeconds(10);
  private final List<DiskSpaceUsageListener> diskSpaceUsageListeners = new ArrayList<>();
  private boolean currentDiskAvailableStatus = true;
  private final DataCfg brokerCfg;

  public DiskSpaceUsageMonitor(final DataCfg brokerCfg) {
    this.brokerCfg = brokerCfg;
  }

  @Override
  protected void onActorStarted() {
    actor.runAtFixedRate(DISK_USAGE_CHECK_DELAY, this::checkDiskUsageAndNotifyListeners);
  }

  private void checkDiskUsageAndNotifyListeners() {
    final long diskSpaceUsage = getDiskSpaceUsage(brokerCfg);
    final boolean available = diskSpaceUsage >= ByteValue.ofGigabytes(4);
    if (currentDiskAvailableStatus != available) {
      currentDiskAvailableStatus = available;
      if (!available) {
        LOG.debug(
            "Out of disk space. Current available {}. Minimum needed {}",
            diskSpaceUsage,
            ByteValue.ofGigabytes(4));
        diskSpaceUsageListeners.forEach(
            DiskSpaceUsageListener::onDiskSpaceUsageIncreasedAboveThreshold);
      } else {
        LOG.debug("Disk space available again. Current available {}", diskSpaceUsage);
        diskSpaceUsageListeners.forEach(
            DiskSpaceUsageListener::onDiskSpaceUsageReducedBelowThreshold);
      }
    }
  }

  private long getDiskSpaceUsage(final DataCfg dataCfg) {
    final var directory = new File(dataCfg.getDirectories().get(0));
    return directory.getUsableSpace();
  }

  public void addDiskUsageListener(final DiskSpaceUsageListener listener) {
    diskSpaceUsageListeners.add(listener);
  }

  public void removeDiskUsageListener(final DiskSpaceUsageListener listener) {
    diskSpaceUsageListeners.remove(listener);
  }
}
