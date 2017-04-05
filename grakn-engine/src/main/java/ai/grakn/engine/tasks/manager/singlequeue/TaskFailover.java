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
 */

package ai.grakn.engine.tasks.manager.singlequeue;

import ai.grakn.engine.TaskId;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.util.EngineID;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static ai.grakn.engine.TaskStatus.RUNNING;
import static ai.grakn.engine.tasks.config.KafkaTerms.NEW_TASKS_TOPIC;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.ALL_ENGINE_WATCH_PATH;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;
import static java.util.stream.Collectors.toSet;

/**
 * <p>
 * Re-schedule tasks that were running when an instance of Engine failed
 * </p>
 *
 * @author Denis lobanov
 */
//TODO Re-write this because it is awful
public class TaskFailover implements TreeCacheListener, AutoCloseable {
    private final Logger LOG = LoggerFactory.getLogger(TaskFailover.class);

    private final TaskStateStorage stateStorage;
    private final CountDownLatch blocker;
    private final TreeCache cache;
    private final Producer<TaskId, TaskState> producer;

    private boolean isAlive = false;

    public TaskFailover(CuratorFramework client, TaskStateStorage stateStorage, Producer<TaskId, TaskState> producer) throws Exception {
        this.stateStorage = stateStorage;
        this.blocker = new CountDownLatch(1);
        this.cache = new TreeCache(client, ALL_ENGINE_WATCH_PATH);

        this.cache.getListenable().addListener(this);
        this.cache.getUnhandledErrorListenable().addListener((message, e) -> {
            LOG.error("error", e);
            blocker.countDown();
        });
        this.cache.start();

        this.producer = producer;

        isAlive = true;
    }

    @Override
    public void close() {
        noThrow(producer::flush, "Could not flush Kafka Producer.");
        noThrow(producer::close, "Could not close Kafka Producer.");
        noThrow(cache::close, "Error closing zookeeper cache");

        isAlive = false;
    }

    /**
     * The TaskRunners should only start/add themselves once a failover is alive
     */
    public boolean isAlive(){
        return isAlive;
    }

    /**
     * Block until this instance of {@link TaskFailover} fails with an unhandled error.
     */
    public void await(){
        try {
            blocker.await();
        } catch (InterruptedException e) {
            LOG.debug("Awaiting failover interrupted");
        }
    }

    /**
     * We want to requeue anything that is not on one of the currently running engines
     */
    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
        Map<String, ChildData> nodes = cache.getCurrentChildren(ALL_ENGINE_WATCH_PATH);

        switch (event.getType()) {
            case NODE_ADDED:
                LOG.debug("New engine joined pool. Current engines: " + nodes.keySet());
                break;
            case NODE_REMOVED:
                Set<EngineID> engines = nodes.keySet().stream().map(EngineID::of).collect(toSet());
                LOG.debug("Engine failure detected. Requeueing anything not on: {}", engines);
                reQueue(engines);
                break;
            default:
                break;
        }
    }

    /**
     * Request all RUNNING tasks from storage and re-submit any that
     * were running on an engine that is no longer alive
     *
     * @param engineIds Set of engines that are currently running
     */
    private void reQueue(Set<EngineID> engineIds) {
        // Get list of tasks that were being processed
        Set<TaskState> runningTasks = stateStorage
                .getTasks(RUNNING, null, null, null, Integer.MAX_VALUE, 0);

        LOG.debug("Found {} RUNNING TASKS ", runningTasks.size());

        // Re-queue all of the IDs.
        for (TaskState task : runningTasks) {

            if (!engineIds.contains(task.engineID())) {
                LOG.debug("Engine {} stopped, task {} requeued", task.engineID(), task.getId());
                producer.send(new ProducerRecord<>(NEW_TASKS_TOPIC, task.getId(), task));
                producer.flush();
            }
        }
    }
}
