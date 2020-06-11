/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.gateway.impl.job;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.zeebe.gateway.EndpointManager;
import io.zeebe.gateway.Loggers;
import io.zeebe.gateway.RequestMapper;
import io.zeebe.gateway.ResponseMapper;
import io.zeebe.gateway.impl.broker.BrokerClient;
import io.zeebe.gateway.impl.broker.cluster.BrokerClusterState;
import io.zeebe.gateway.impl.broker.request.BrokerActivateJobsRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.ActivateJobsRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.ActivateJobsResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Iterates in round-robin fashion over partitions to activate jobs. Uses a map from job type to
 * partition-IDs to determine the next partition to use.
 */
public final class RoundRobinActivateJobsHandler implements ActivateJobsHandler {

  private final Map<String, Integer> jobTypeToNextPartitionId = new HashMap<>();
  private final BrokerClient brokerClient;

  public RoundRobinActivateJobsHandler(final BrokerClient brokerClient) {
    this.brokerClient = brokerClient;
  }

  @Override
  public void activateJobs(
      final ActivateJobsRequest request,
      final StreamObserver<ActivateJobsResponse> responseObserver) {
    final BrokerClusterState topology = brokerClient.getTopologyManager().getTopology();
    if (topology != null) {
      final int partitionsCount = topology.getPartitionsCount();
      activateJobs(
          partitionsCount,
          RequestMapper.toActivateJobsRequest(request),
          request.getMaxJobsToActivate(),
          request.getType(),
          responseObserver::onNext,
          remainingAmount -> responseObserver.onCompleted());
    }
  }

  public void activateJobs(
      final int partitionsCount,
      final BrokerActivateJobsRequest request,
      final int maxJobsToActivate,
      final String type,
      final Consumer<ActivateJobsResponse> onResponse,
      final Consumer<Integer> onCompleted) {
    activateJobs(
        request,
        partitionIdIteratorForType(type, partitionsCount),
        maxJobsToActivate,
        type,
        onResponse,
        onCompleted);
  }

  private void activateJobs(
      final BrokerActivateJobsRequest request,
      final PartitionIdIterator partitionIdIterator,
      final int remainingAmount,
      final String jobType,
      final Consumer<ActivateJobsResponse> onResponse,
      final Consumer<Integer> onCompleted) {
    activateJobs(
        request, partitionIdIterator, remainingAmount, jobType, onResponse, onCompleted, false);
  }

  private void activateJobs(
      final BrokerActivateJobsRequest request,
      final PartitionIdIterator partitionIdIterator,
      final int remainingAmount,
      final String jobType,
      final Consumer<ActivateJobsResponse> onResponse,
      final Consumer<Integer> onCompleted,
      final boolean pollPrevPartition) {

    if (remainingAmount > 0 && (pollPrevPartition || partitionIdIterator.hasNext())) {
      final int partitionId =
          pollPrevPartition
              ? partitionIdIterator.getCurrentPartitionId()
              : partitionIdIterator.next();

      // partitions to check and jobs to activate left
      request.setPartitionId(partitionId);
      request.setMaxJobsToActivate(remainingAmount);
      brokerClient.sendRequest(
          request,
          (key, response) -> {
            final ActivateJobsResponse grpcResponse =
                ResponseMapper.toActivateJobsResponse(key, response);
            final int jobsCount = grpcResponse.getJobsCount();
            if (jobsCount > 0) {
              onResponse.accept(grpcResponse);
            }

            activateJobs(
                request,
                partitionIdIterator,
                remainingAmount - jobsCount,
                jobType,
                onResponse,
                onCompleted,
                response.getTruncated());
          },
          error -> {
            logErrorResponse(partitionIdIterator, jobType, error);
            activateJobs(
                request, partitionIdIterator, remainingAmount, jobType, onResponse, onCompleted);
          });
    } else {
      // enough jobs activated or no more partitions left to check
      jobTypeToNextPartitionId.put(jobType, partitionIdIterator.getCurrentPartitionId());
      onCompleted.accept(remainingAmount);
    }
  }

  private void logErrorResponse(
      final PartitionIdIterator partitionIdIterator, final String jobType, final Throwable error) {
    final StatusRuntimeException statusRuntimeException = EndpointManager.convertThrowable(error);
    if (statusRuntimeException.getStatus().getCode() != Code.RESOURCE_EXHAUSTED) {
      Loggers.GATEWAY_LOGGER.warn(
          "Failed to activate jobs for type {} from partition {}",
          jobType,
          partitionIdIterator.getCurrentPartitionId(),
          error);
    }
  }

  private PartitionIdIterator partitionIdIteratorForType(
      final String jobType, final int partitionsCount) {
    final Integer nextPartitionId = jobTypeToNextPartitionId.computeIfAbsent(jobType, t -> 0);
    return new PartitionIdIterator(nextPartitionId, partitionsCount);
  }
}
