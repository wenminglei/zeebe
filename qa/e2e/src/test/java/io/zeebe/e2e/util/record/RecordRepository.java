package io.zeebe.e2e.util.record;

import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.RecordValue;
import io.zeebe.protocol.record.ValueType;
import io.zeebe.protocol.record.intent.DeploymentIntent;
import io.zeebe.protocol.record.intent.IncidentIntent;
import io.zeebe.protocol.record.intent.JobBatchIntent;
import io.zeebe.protocol.record.intent.JobIntent;
import io.zeebe.protocol.record.intent.MessageIntent;
import io.zeebe.protocol.record.intent.MessageStartEventSubscriptionIntent;
import io.zeebe.protocol.record.intent.MessageSubscriptionIntent;
import io.zeebe.protocol.record.intent.TimerIntent;
import io.zeebe.protocol.record.intent.VariableDocumentIntent;
import io.zeebe.protocol.record.intent.VariableIntent;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import io.zeebe.protocol.record.intent.WorkflowInstanceSubscriptionIntent;
import io.zeebe.test.util.record.DeploymentRecordStream;
import io.zeebe.test.util.record.IncidentRecordStream;
import io.zeebe.test.util.record.JobBatchRecordStream;
import io.zeebe.test.util.record.JobRecordStream;
import io.zeebe.test.util.record.MessageRecordStream;
import io.zeebe.test.util.record.MessageStartEventSubscriptionRecordStream;
import io.zeebe.test.util.record.MessageSubscriptionRecordStream;
import io.zeebe.test.util.record.RecordStream;
import io.zeebe.test.util.record.TimerRecordStream;
import io.zeebe.test.util.record.VariableDocumentRecordStream;
import io.zeebe.test.util.record.VariableRecordStream;
import io.zeebe.test.util.record.WorkflowInstanceCreationRecordStream;
import io.zeebe.test.util.record.WorkflowInstanceRecordStream;
import io.zeebe.test.util.record.WorkflowInstanceResultRecordStream;
import io.zeebe.test.util.record.WorkflowInstanceSubscriptionRecordStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class RecordRepository {
  private final Lock lock = new ReentrantLock();
  private final Condition emptyCondition = lock.newCondition();
  private final List<Record<?>> records;
  private final Duration timeout;

  public RecordRepository() {
    this(Duration.ofSeconds(5));
  }

  public RecordRepository(final Duration timeout) {
    this(timeout, new ArrayList<>());
  }

  public RecordRepository(final Duration timeout, final List<Record<?>> records) {
    this.timeout = timeout;
    this.records = records;
  }

  public void add(final Record<? extends RecordValue> record) {
    try {
      lock.lockInterruptibly();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }

    try {
      records.add(record.clone());
      emptyCondition.signalAll();
    } finally {
      lock.unlock();
    }
  }

  public void reset() {
    try {
      lock.lockInterruptibly();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }

    try {
      records.clear();
      emptyCondition.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @SuppressWarnings("unchecked")
  protected <T extends RecordValue> Stream<Record<T>> records(final ValueType valueType) {
    return newStream().filter(r -> r.getValueType() == valueType).map(r -> (Record<T>) r);
  }

  @SuppressWarnings("unchecked")
  public RecordStream records() {
    return new RecordStream(newStream().map(r -> (Record<RecordValue>) r));
  }

  public MessageSubscriptionRecordStream messageSubscriptionRecords() {
    return new MessageSubscriptionRecordStream(records(ValueType.MESSAGE_SUBSCRIPTION));
  }

  public MessageSubscriptionRecordStream messageSubscriptionRecords(
      final MessageSubscriptionIntent intent) {
    return messageSubscriptionRecords().withIntent(intent);
  }

  public MessageStartEventSubscriptionRecordStream messageStartEventSubscriptionRecords() {
    return new MessageStartEventSubscriptionRecordStream(
        records(ValueType.MESSAGE_START_EVENT_SUBSCRIPTION));
  }

  public MessageStartEventSubscriptionRecordStream messageStartEventSubscriptionRecords(
      final MessageStartEventSubscriptionIntent intent) {
    return messageStartEventSubscriptionRecords().withIntent(intent);
  }

  public DeploymentRecordStream deploymentRecords() {
    return new DeploymentRecordStream(records(ValueType.DEPLOYMENT));
  }

  public DeploymentRecordStream deploymentRecords(final DeploymentIntent intent) {
    return deploymentRecords().withIntent(intent);
  }

  public JobRecordStream jobRecords() {
    return new JobRecordStream(records(ValueType.JOB));
  }

  public JobRecordStream jobRecords(final JobIntent intent) {
    return jobRecords().withIntent(intent);
  }

  public JobBatchRecordStream jobBatchRecords() {
    return new JobBatchRecordStream(records(ValueType.JOB_BATCH));
  }

  public JobBatchRecordStream jobBatchRecords(final JobBatchIntent intent) {
    return jobBatchRecords().withIntent(intent);
  }

  public IncidentRecordStream incidentRecords() {
    return new IncidentRecordStream(records(ValueType.INCIDENT));
  }

  public IncidentRecordStream incidentRecords(final IncidentIntent intent) {
    return incidentRecords().withIntent(intent);
  }

  public WorkflowInstanceSubscriptionRecordStream workflowInstanceSubscriptionRecords() {
    return new WorkflowInstanceSubscriptionRecordStream(
        records(ValueType.WORKFLOW_INSTANCE_SUBSCRIPTION));
  }

  public WorkflowInstanceSubscriptionRecordStream workflowInstanceSubscriptionRecords(
      final WorkflowInstanceSubscriptionIntent intent) {
    return workflowInstanceSubscriptionRecords().withIntent(intent);
  }

  public MessageRecordStream messageRecords() {
    return new MessageRecordStream(records(ValueType.MESSAGE));
  }

  public MessageRecordStream messageRecords(final MessageIntent intent) {
    return messageRecords().withIntent(intent);
  }

  public WorkflowInstanceRecordStream workflowInstanceRecords() {
    return new WorkflowInstanceRecordStream(records(ValueType.WORKFLOW_INSTANCE));
  }

  public WorkflowInstanceRecordStream workflowInstanceRecords(final WorkflowInstanceIntent intent) {
    return workflowInstanceRecords().withIntent(intent);
  }

  public TimerRecordStream timerRecords() {
    return new TimerRecordStream(records(ValueType.TIMER));
  }

  public TimerRecordStream timerRecords(final TimerIntent intent) {
    return timerRecords().withIntent(intent);
  }

  public VariableRecordStream variableRecords() {
    return new VariableRecordStream(records(ValueType.VARIABLE));
  }

  public VariableRecordStream variableRecords(final VariableIntent intent) {
    return variableRecords().withIntent(intent);
  }

  public VariableDocumentRecordStream variableDocumentRecords() {
    return new VariableDocumentRecordStream(records(ValueType.VARIABLE_DOCUMENT));
  }

  public VariableDocumentRecordStream variableDocumentRecords(final VariableDocumentIntent intent) {
    return variableDocumentRecords().withIntent(intent);
  }

  public WorkflowInstanceCreationRecordStream workflowInstanceCreationRecords() {
    return new WorkflowInstanceCreationRecordStream(records(ValueType.WORKFLOW_INSTANCE_CREATION));
  }

  public WorkflowInstanceResultRecordStream workflowInstanceResultRecords() {
    return new WorkflowInstanceResultRecordStream(records(ValueType.WORKFLOW_INSTANCE_RESULT));
  }

  private Stream<Record<?>> newStream() {
    final var recordIterator = new RecordIterator(records, lock, emptyCondition, timeout);
    final var spliterator =
        Spliterators.spliteratorUnknownSize(recordIterator, Spliterator.ORDERED);
    return StreamSupport.stream(spliterator, false);
  }
}
