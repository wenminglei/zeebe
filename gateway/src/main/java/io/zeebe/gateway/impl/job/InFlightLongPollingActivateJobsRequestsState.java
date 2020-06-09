/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.gateway.impl.job;

import io.zeebe.gateway.Loggers;
import io.zeebe.gateway.metrics.LongPollingMetrics;
import java.util.LinkedList;
import java.util.Queue;
import org.slf4j.Logger;

public final class InFlightLongPollingActivateJobsRequestsState {

  private static final Logger LOGGER = Loggers.GATEWAY_LOGGER;

  private final String jobType;
  private final LongPollingMetrics metrics;
  private final Queue<LongPollingActivateJobsRequest> pendingRequests = new LinkedList<>();
  private LongPollingActivateJobsRequest activeRequest;
  private int failedAttempts;
  private long lastUpdatedTime;

  public InFlightLongPollingActivateJobsRequestsState(
      final String jobType, final LongPollingMetrics metrics) {
    this.jobType = jobType;
    this.metrics = metrics;
  }

  public void incrementFailedAttempts(final long lastUpdatedTime) {
    failedAttempts++;
    this.lastUpdatedTime = lastUpdatedTime;
  }

  public void resetFailedAttempts(final int failedAttempts) {
    this.failedAttempts = failedAttempts;
  }

  public int getFailedAttempts() {
    return failedAttempts;
  }

  public long getLastUpdatedTime() {
    return lastUpdatedTime;
  }

  public synchronized void enqueueRequest(final LongPollingActivateJobsRequest request) {
    pendingRequests.offer(request);
    removeObsoleteRequestsAndUpdateMetrics();
  }

  private synchronized void removeObsoleteRequestsAndUpdateMetrics() {
    pendingRequests.removeIf(this::isObsolete);
    metrics.setBlockedRequestsCount(jobType, pendingRequests.size());
    if (activeRequest != null && isObsolete(activeRequest)) {
      removeActiveRequest();
    }
  }

  private boolean isObsolete(final LongPollingActivateJobsRequest request) {
    return request.isTimedOut() || request.isCanceled() || request.isCompleted();
  }

  public synchronized void removeRequest(final LongPollingActivateJobsRequest request) {
    if (activeRequest == request) {
      removeActiveRequest();
    }
    pendingRequests.remove(request);
    removeObsoleteRequestsAndUpdateMetrics();
  }

  public synchronized LongPollingActivateJobsRequest getNextPendingRequest() {
    removeObsoleteRequestsAndUpdateMetrics();
    final LongPollingActivateJobsRequest request = pendingRequests.poll();
    metrics.setBlockedRequestsCount(jobType, pendingRequests.size());
    return request;
  }

  public synchronized LongPollingActivateJobsRequest getActiveRequest() {
    if (activeRequest != null && isObsolete(activeRequest)) {
      removeActiveRequest();
    }
    return activeRequest;
  }

  public synchronized void setActiveRequest(final LongPollingActivateJobsRequest request) {
    if (activeRequest != null) {
      LOGGER.error(
          "SetActiveRequest - Active request is already set[activeRequest="
              + activeRequest
              + ", request="
              + request);
    }
    this.activeRequest = request;
  }

  public synchronized void removeActiveRequest() {
    if (activeRequest == null) {
      LOGGER.error("RemoveActiveRequest -No active request present");
    }
    activeRequest = null;
  }
}
