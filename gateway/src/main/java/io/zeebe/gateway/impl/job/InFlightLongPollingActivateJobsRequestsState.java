/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.gateway.impl.job;

import io.zeebe.gateway.metrics.LongPollingMetrics;
import java.util.LinkedList;
import java.util.Queue;

public final class InFlightLongPollingActivateJobsRequestsState {

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
    metrics.setBlockedRequestsCount(jobType, pendingRequests.size());
  }

  public synchronized void removeCanceledRequests() {
    pendingRequests.removeIf(LongPollingActivateJobsRequest::isCanceled);
    metrics.setBlockedRequestsCount(jobType, pendingRequests.size());
  }

  public synchronized void removeRequest(final LongPollingActivateJobsRequest request) {
    pendingRequests.remove(request);
    metrics.setBlockedRequestsCount(jobType, pendingRequests.size());
  }

  public synchronized LongPollingActivateJobsRequest getNextPendingRequest() {
    final LongPollingActivateJobsRequest request = pendingRequests.poll();
    metrics.setBlockedRequestsCount(jobType, pendingRequests.size());
    return request;
  }

  public synchronized LongPollingActivateJobsRequest getActiveRequest() {
    return activeRequest;
  }

  public synchronized void setActiveRequest(final LongPollingActivateJobsRequest request) {
    if (activeRequest != null) {
      throw new IllegalStateException(
          "Active request is already set[activeRequest=" + activeRequest + ", request=" + request);
    }
    this.activeRequest = request;
  }

  public synchronized void removeActiveRequest() {
    if (activeRequest == null) {
      throw new IllegalStateException("No active request present");
    }
    activeRequest = null;
  }
}
