/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Ltd
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

package ai.grakn.engine.backgroundtasks.distributed;

import ai.grakn.engine.backgroundtasks.StateStorage;
import ai.grakn.engine.backgroundtasks.taskstorage.GraknStateStorage;
import ai.grakn.engine.backgroundtasks.taskstorage.SynchronizedState;
import ai.grakn.engine.backgroundtasks.taskstorage.SynchronizedStateStorage;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;

import java.util.*;

import static ai.grakn.engine.backgroundtasks.TaskStatus.RUNNING;
import static ai.grakn.engine.backgroundtasks.TaskStatus.SCHEDULED;
import static ai.grakn.engine.backgroundtasks.config.ConfigHelper.kafkaProducer;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.WORK_QUEUE_TOPIC;
import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.*;

public class TaskFailover implements TreeCacheListener {
    private Map<String, ChildData> current;
    private TreeCache cache;
    private KafkaProducer<String, String> producer;
    private StateStorage stateStorage;
    private SynchronizedStateStorage synchronizedStateStorage;
    private final KafkaLogger LOG = KafkaLogger.getInstance();

    public TaskFailover(CuratorFramework client, TreeCache cache) throws Exception {
        this.cache = cache;
        current = cache.getCurrentChildren(RUNNERS_WATCH);

        producer = kafkaProducer();
        stateStorage = new GraknStateStorage();
        synchronizedStateStorage = SynchronizedStateStorage.getInstance();
        scanStaleStates(client);
    }

    public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
        Map<String, ChildData> nodes = cache.getCurrentChildren(RUNNERS_WATCH);

        switch (event.getType()) {
            case NODE_ADDED:
                LOG.debug("New engine joined pool.");
                current = nodes;
                break;
            case NODE_REMOVED:
                LOG.debug("Engine failure detected.");
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
            // Dead TaskRunner
            if(!nodes.containsKey(engineId)) {
                LOG.debug("Dead engine: "+engineId);
                reQueue(client, engineId);
            }
        }
    }

    /**
     * Re-submit all tasks to the work queue that a dead TaskRunner was working on.
     * @param client CuratorFramework
     * @param engineID String unique ID of engine
     * @throws Exception
     */
    private void reQueue(CuratorFramework client, String engineID) throws Exception {
        // Get list of task last processed by this TaskRunner
        byte[] b = client.getData().forPath(RUNNERS_STATE+"/"+engineID);

        // Re-queue all of the IDs.
        JSONArray ids = new JSONArray(new String(b));
        for(Object o: ids) {
            String id = (String)o;

            // Mark task as SCHEDULED again.
            synchronizedStateStorage.updateState(id, SCHEDULED, "", null);

            String configuration = stateStorage.getState(id)
                                               .configuration()
                                               .toString();

            producer.send(new ProducerRecord<>(WORK_QUEUE_TOPIC, id, configuration));
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
            SynchronizedState state = synchronizedStateStorage.getState(id);

            if(state.status() != RUNNING)
                break;

            String engineId = state.engineID();
            if(engineId == null || engineId.isEmpty())
                throw new IllegalStateException("ZK Task SynchronizedState - "+id+" - has no engineID ("+engineId+") - status "+state.status().toString());

            // Avoid further calls to ZK if we already know about this one.
            if(deadRunners.contains(engineId))
                break;

            // Check if assigned engine is still alive
            if(client.checkExists().forPath(RUNNERS_WATCH+"/"+engineId) == null) {
                reQueue(client, engineId);
                deadRunners.add(engineId);
            }
        }
    }
}
