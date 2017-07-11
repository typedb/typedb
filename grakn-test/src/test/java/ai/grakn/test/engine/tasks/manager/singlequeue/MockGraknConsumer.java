/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */

package ai.grakn.test.engine.tasks.manager.singlequeue;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * This is a mock Kafka consumer, copied from {@link org.apache.kafka.clients.consumer.MockConsumer}.
 *
 * This class has been copied to add extra functionality such as running a task when a poll is empty.
 */
public class MockGraknConsumer<K, V> implements Consumer<K, V> {

    private final Map<String, List<PartitionInfo>> partitions;
    private final SubscriptionState subscriptions;
    private Map<TopicPartition, List<ConsumerRecord<K, V>>> records;
    private Set<TopicPartition> paused;
    private boolean closed;
    private final Map<TopicPartition, Long> beginningOffsets;
    private final Map<TopicPartition, Long> endOffsets;

    private Queue<Runnable> pollTasks;
    private KafkaException exception;

    private AtomicBoolean wakeup;
    private Runnable emptyPollTask = null;

    public MockGraknConsumer(OffsetResetStrategy offsetResetStrategy) {
        this.subscriptions = new SubscriptionState(offsetResetStrategy);
        this.partitions = new HashMap<>();
        this.records = new HashMap<>();
        this.paused = new HashSet<>();
        this.closed = false;
        this.beginningOffsets = new HashMap<>();
        this.endOffsets = new HashMap<>();
        this.pollTasks = new LinkedList<>();
        this.exception = null;
        this.wakeup = new AtomicBoolean(false);
    }

    @Override
    public synchronized Set<TopicPartition> assignment() {
        return this.subscriptions.assignedPartitions();
    }

    /** Simulate a rebalance event. */
    public void rebalance(Collection<TopicPartition> newAssignment) {
        // TODO: Rebalance callbacks
        this.records.clear();
        this.subscriptions.assignFromSubscribed(newAssignment);
    }

    @Override
    public synchronized Set<String> subscription() {
        return this.subscriptions.subscription();
    }

    @Override
    public synchronized void subscribe(Collection<String> topics) {
        subscribe(topics, new NoOpConsumerRebalanceListener());
    }

    @Override
    public synchronized void subscribe(Pattern pattern, final ConsumerRebalanceListener listener) {
        ensureNotClosed();
        this.subscriptions.subscribe(pattern, listener);
        Set<String> topicsToSubscribe = new HashSet<>();
        for (String topic: partitions.keySet()) {
            if (pattern.matcher(topic).matches() &&
                    !subscriptions.subscription().contains(topic))
                topicsToSubscribe.add(topic);
        }
        ensureNotClosed();
        this.subscriptions.subscribeFromPattern(topicsToSubscribe);
    }

    @Override
    public synchronized void subscribe(Collection<String> topics, final ConsumerRebalanceListener listener) {
        ensureNotClosed();
        this.subscriptions.subscribe(new HashSet<>(topics), listener);
    }

    @Override
    public synchronized void assign(Collection<TopicPartition> partitions) {
        ensureNotClosed();
        this.subscriptions.assignFromUser(new HashSet<>(partitions));
    }

    @Override
    public synchronized void unsubscribe() {
        ensureNotClosed();
        subscriptions.unsubscribe();
    }

    @Override
    public synchronized ConsumerRecords<K, V> poll(long timeout) {
        ensureNotClosed();

        // Synchronize around the entire execution so new tasks to be triggered on subsequent poll calls can be added in
        // the callback
        synchronized (pollTasks) {
            Runnable task = pollTasks.poll();
            if (task != null)
                task.run();
        }

        if (wakeup.get()) {
            wakeup.set(false);
            throw new WakeupException();
        }

        if (exception != null) {
            RuntimeException exception = this.exception;
            this.exception = null;
            throw exception;
        }

        // Handle seeks that need to wait for a poll() call to be processed
        for (TopicPartition tp : subscriptions.missingFetchPositions())
            updateFetchPosition(tp);

        // update the consumed offset
        for (Map.Entry<TopicPartition, List<ConsumerRecord<K, V>>> entry : this.records.entrySet()) {
            if (!subscriptions.isPaused(entry.getKey())) {
                List<ConsumerRecord<K, V>> recs = entry.getValue();
                if (!recs.isEmpty())
                    this.subscriptions.position(entry.getKey(), recs.get(recs.size() - 1).offset() + 1);
            }
        }

        ConsumerRecords<K, V> copy = new ConsumerRecords<K, V>(this.records);
        this.records = new HashMap<TopicPartition, List<ConsumerRecord<K, V>>>();

        if (copy.count() == 0 && emptyPollTask != null) emptyPollTask.run();

        return copy;
    }

    public void addRecord(ConsumerRecord<K, V> record) {
        ensureNotClosed();
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        Set<TopicPartition> currentAssigned = new HashSet<>(this.subscriptions.assignedPartitions());
        if (!currentAssigned.contains(tp))
            throw new IllegalStateException("Cannot add records for a partition that is not assigned to the consumer");
        List<ConsumerRecord<K, V>> recs = this.records.get(tp);
        if (recs == null) {
            recs = new ArrayList<ConsumerRecord<K, V>>();
            this.records.put(tp, recs);
        }
        recs.add(record);
    }

    public void setException(KafkaException exception) {
        this.exception = exception;
    }

    @Override
    public synchronized void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        ensureNotClosed();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet())
            subscriptions.committed(entry.getKey(), entry.getValue());
        if (callback != null) {
            callback.onComplete(offsets, null);
        }
    }

    @Override
    public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        commitAsync(offsets, null);
    }

    @Override
    public synchronized void commitAsync() {
        commitAsync(null);
    }

    @Override
    public synchronized void commitAsync(OffsetCommitCallback callback) {
        ensureNotClosed();
        commitAsync(this.subscriptions.allConsumed(), callback);
    }

    @Override
    public synchronized void commitSync() {
        commitSync(this.subscriptions.allConsumed());
    }

    @Override
    public synchronized void seek(TopicPartition partition, long offset) {
        ensureNotClosed();
        subscriptions.seek(partition, offset);
    }

    @Override
    public synchronized OffsetAndMetadata committed(TopicPartition partition) {
        ensureNotClosed();
        return subscriptions.committed(partition);
    }

    @Override
    public synchronized long position(TopicPartition partition) {
        ensureNotClosed();
        if (!this.subscriptions.isAssigned(partition))
            throw new IllegalArgumentException("You can only check the position for partitions assigned to this consumer.");
        Long offset = this.subscriptions.position(partition);
        if (offset == null) {
            updateFetchPosition(partition);
            offset = this.subscriptions.position(partition);
        }
        return offset;
    }

    @Override
    public synchronized void seekToBeginning(Collection<TopicPartition> partitions) {
        ensureNotClosed();
        for (TopicPartition tp : partitions)
            subscriptions.needOffsetReset(tp, OffsetResetStrategy.EARLIEST);
    }

    public void updateBeginningOffsets(Map<TopicPartition, Long> newOffsets) {
        beginningOffsets.putAll(newOffsets);
    }

    @Override
    public synchronized void seekToEnd(Collection<TopicPartition> partitions) {
        ensureNotClosed();
        for (TopicPartition tp : partitions)
            subscriptions.needOffsetReset(tp, OffsetResetStrategy.LATEST);
    }

    public void updateEndOffsets(Map<TopicPartition, Long> newOffsets) {
        endOffsets.putAll(newOffsets);
    }

    @Override
    public synchronized Map<MetricName, ? extends Metric> metrics() {
        ensureNotClosed();
        return Collections.emptyMap();
    }

    @Override
    public synchronized List<PartitionInfo> partitionsFor(String topic) {
        ensureNotClosed();
        return this.partitions.get(topic);
    }

    @Override
    public synchronized Map<String, List<PartitionInfo>> listTopics() {
        ensureNotClosed();
        return partitions;
    }

    public void updatePartitions(String topic, List<PartitionInfo> partitions) {
        ensureNotClosed();
        this.partitions.put(topic, partitions);
    }

    @Override
    public synchronized void pause(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            subscriptions.pause(partition);
            paused.add(partition);
        }
    }

    @Override
    public synchronized void resume(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            subscriptions.resume(partition);
            paused.remove(partition);
        }
    }

    @Override
    public synchronized Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public synchronized Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        Map<TopicPartition, Long> result = new HashMap<>();
        for (TopicPartition tp : partitions) {
            Long beginningOffset = beginningOffsets.get(tp);
            if (beginningOffset == null)
                throw new IllegalStateException("The partition " + tp + " does not have a beginning offset.");
            result.put(tp, beginningOffset);
        }
        return result;
    }

    @Override
    public synchronized Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        Map<TopicPartition, Long> result = new HashMap<>();
        for (TopicPartition tp : partitions) {
            Long endOffset = endOffsets.get(tp);
            if (endOffset == null)
                throw new IllegalStateException("The partition " + tp + " does not have an end offset.");
            result.put(tp, endOffset);
        }
        return result;
    }

    @Override
    public synchronized void close() {
        ensureNotClosed();
        this.closed = true;
    }

    public boolean closed() {
        return this.closed;
    }

    @Override
    public synchronized void wakeup() {
        wakeup.set(true);
    }

    /**
     * Schedule a task to be executed during a poll(). One enqueued task will be executed per {@link #poll(long)}
     * invocation. You can use this repeatedly to mock out multiple responses to poll invocations.
     * @param task the task to be executed
     */
    public void schedulePollTask(Runnable task) {
        synchronized (pollTasks) {
            pollTasks.add(task);
        }
    }

    public void scheduleEmptyPollTask(Runnable task) {
        emptyPollTask = task;
    }

    public void scheduleNopPollTask() {
        schedulePollTask(new Runnable() {
            @Override
            public void run() {
                // noop
            }
        });
    }

    public Set<TopicPartition> paused() {
        return Collections.unmodifiableSet(new HashSet<>(paused));
    }

    private void ensureNotClosed() {
        if (this.closed)
            throw new IllegalStateException("This consumer has already been closed.");
    }

    private void updateFetchPosition(TopicPartition tp) {
        if (subscriptions.isOffsetResetNeeded(tp)) {
            resetOffsetPosition(tp);
        } else if (subscriptions.committed(tp) == null) {
            subscriptions.needOffsetReset(tp);
            resetOffsetPosition(tp);
        } else {
            subscriptions.seek(tp, subscriptions.committed(tp).offset());
        }
    }

    private void resetOffsetPosition(TopicPartition tp) {
        OffsetResetStrategy strategy = subscriptions.resetStrategy(tp);
        Long offset;
        if (strategy == OffsetResetStrategy.EARLIEST) {
            offset = beginningOffsets.get(tp);
            if (offset == null)
                throw new IllegalStateException("MockConsumer didn't have beginning offset specified, but tried to seek to beginning");
        } else if (strategy == OffsetResetStrategy.LATEST) {
            offset = endOffsets.get(tp);
            if (offset == null)
                throw new IllegalStateException("MockConsumer didn't have end offset specified, but tried to seek to end");
        } else {
            throw new NoOffsetForPartitionException(tp);
        }
        seek(tp, offset);
    }
}
