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

package ai.grakn.engine.tasks.manager.multiqueue;

import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.TaskState;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static ai.grakn.engine.TaskStatus.RUNNING;
import static ai.grakn.engine.TaskStatus.SCHEDULED;
import static ai.grakn.engine.tasks.config.ConfigHelper.kafkaProducer;
import static ai.grakn.engine.tasks.config.KafkaTerms.WORK_QUEUE_TOPIC;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.RUNNERS_STATE;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.RUNNERS_WATCH;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.TASKS_PATH_PREFIX;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;
import static java.lang.String.format;

/**
 * <p>
 * Re-schedule tasks that were running when an instance of Engine failed
 * </p>
 *
 * @author Denis lobanov
 */
public class TaskFailover implements TreeCacheListener, AutoCloseable {
    private final Logger LOG = LoggerFactory.getLogger(TaskFailover.class);

    private final TaskStateStorage stateStorage;
    private final CountDownLatch blocker;
    private final TreeCache cache;

    private Map<String, ChildData> current;
    private KafkaProducer<String, String> producer;

    public TaskFailover(CuratorFramework client, TaskStateStorage stateStorage) throws Exception {
        this.stateStorage = stateStorage;
        this.blocker = new CountDownLatch(1);
        this.cache = new TreeCache(client, RUNNERS_WATCH);

        this.cache.getListenable().addListener(this);
        this.cache.getUnhandledErrorListenable().addListener((message, e) -> blocker.countDown());

        current = cache.getCurrentChildren(RUNNERS_WATCH);
        producer = kafkaProducer();
        scanStaleStates(client);
    }

    @Override
    public void close() {
        noThrow(producer::flush, "Could not flush Kafka Producer.");
        noThrow(producer::close, "Could not close Kafka Producer.");
        noThrow(cache::close, "Error closing zookeeper cache");
    }

    /**
     * Block until this instance of {@link TaskFailover} fails with an unhandled error.
     */
    public void await(){
        try {
            blocker.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
        Map<String, ChildData> nodes = cache.getCurrentChildren(RUNNERS_WATCH);

        switch (event.getType()) {
            case NODE_ADDED:
                LOG.debug("New engine joined pool. Current engines: " + nodes.keySet());
                current = nodes;
                break;
            case NODE_REMOVED:
                LOG.debug("Engine failure detected. Current engines " + nodes.keySet());
                failover(client, nodes);
                current = nodes;
                break;
            default:
                break;
        }
    }

    /**
     * Find diff between current and @nodes to figure out which engines died. Calls reQueue to resubmit all tasks they were
     * assigned to to Kafka work queue.
     * @param client CuratorFramework
     * @param nodes Map<String, ChildData> of all currently alive ZNodes, a diff between this and cached @current is
     *              used to figure out which Engines died.
     * @throws Exception
     */
    private void failover(CuratorFramework client, Map<String, ChildData> nodes) throws Exception {
        for(String engineId: current.keySet()) {
            // Dead MultiQueueTaskRunner
            if(!nodes.containsKey(engineId)) {
                LOG.debug("Dead engine: "+engineId);
                reQueue(client, engineId);
            }
        }
    }

    /**
     * Re-submit all tasks to the work queue that a dead MultiQueueTaskRunner was working on.
     * @param client CuratorFramework
     * @param engineID String unique ID of engine
     * @throws Exception
     */
    private void reQueue(CuratorFramework client, String engineID) throws Exception {
        // Get list of task last processed by this MultiQueueTaskRunner
        byte[] b = client.getData().forPath(RUNNERS_STATE+"/"+engineID);

        // Re-queue all of the IDs.
        JSONArray ids = new JSONArray(new String(b, StandardCharsets.UTF_8));
        for(Object o: ids) {
            String id = (String)o;

            // Mark task as SCHEDULED again
            TaskState taskState = stateStorage.getState(id);

            if(taskState.status() == RUNNING) {
                LOG.debug(format("Engine [%s] stopped, task [%s] requeued", engineID, taskState.getId()));
                stateStorage.updateState(taskState.status(SCHEDULED));
                producer.send(new ProducerRecord<>(WORK_QUEUE_TOPIC, id, taskState.configuration().toString()));
            } else {
                LOG.debug(format("Engine [%s] stopped, task [%s] not restarted because state [%s]"
                        , engineID, taskState.getId(), taskState.status()));
            }
        }
    }

    /**
     * Go through all the task states and check for any marked RUNNING with engineIDs that no longer exist in our watch
     * path (i.e. dead engines).
     * @param client CuratorFramework
     */
    private void scanStaleStates(CuratorFramework client) throws Exception {
        Set<String> deadRunners = new HashSet<>();

        for(String id: client.getChildren().forPath(TASKS_PATH_PREFIX)) {
            TaskState state = stateStorage.getState(id);

            if(state.status() != RUNNING) {
                break;
            }

            String engineId = state.engineID();
            if(engineId == null || engineId.isEmpty()) {
                throw new IllegalStateException("ZK Task SynchronizedState - " + id + " - has no engineID (" + engineId + ") - status " + state.status().toString());
            }

            // Avoid further calls to ZK if we already know about this one.
            if(deadRunners.contains(engineId)) {
                break;
            }

            // Check if assigned engine is still alive
            if(client.checkExists().forPath(RUNNERS_WATCH+"/"+engineId) == null) {
                reQueue(client, engineId);
                deadRunners.add(engineId);
            }
        }
    }
}
