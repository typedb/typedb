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

package ai.grakn.engine.postprocessing;

import ai.grakn.concept.ConceptId;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.engine.tasks.TaskConfiguration;
import ai.grakn.engine.tasks.TaskSchedule;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskSubmitter;
import ai.grakn.engine.tasks.connection.RedisConnection;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.util.REST;
import mjson.Json;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * <p>
 *     Task that controls when types are updated with their new instance counts
 * </p>
 *
 * <p>
 *     This task begins only if enough time has passed (configurable) since the last time a job was added.
 * </p>
 *
 * @author fppt
 */
public class UpdatingInstanceCountTask extends BackgroundTask {
    public static final RedisConnection redis = RedisConnection.getConnection();
    public static final GraknEngineConfig CONFIG = GraknEngineConfig.getInstance();
    private static final long SHARDING_THRESHOLD = CONFIG.getPropertyAsLong(AbstractGraknGraph.SHARDING_THRESHOLD);
    private static final EngineGraknGraphFactory FACTORY = EngineGraknGraphFactory.create(CONFIG.getProperties());

    @Override
    public boolean start(Consumer<TaskCheckpoint> saveCheckpoint, TaskConfiguration configuration, TaskSubmitter taskSubmitter) {
        Map<ConceptId, Long> jobs = getCountUpdatingJobs(configuration);
        String keyspace = configuration.json().at(REST.Request.KEYSPACE).asString();

        //We Use redis to keep track of counts in order to ensure sharding happens in a centralised manner.
        //The graph cannot be used because each engine can have it's own snapshot of the graph with caching which makes
        //values only approximately correct
        Set<ConceptId> conceptToShard = new HashSet<>();

        //Update counts
        jobs.forEach((key, value) -> {
            if(updateShardCounts(keyspace, key, value)) conceptToShard.add(key);
        });

        //Shard anything which requires sharding
        conceptToShard.forEach(type -> shardConcept(keyspace, type));

        return true;
    }

    /**
     * Extracts the type labels and count from the Json configuration
     * @param configuration The configuration which contains types counts
     * @return A map indicating the number of instances each type has gained or lost
     */
    private static Map<ConceptId, Long> getCountUpdatingJobs(TaskConfiguration configuration){
        return  configuration.json().at(REST.Request.COMMIT_LOG_COUNTING).asJsonList().stream()
                .collect(Collectors.toMap(
                        e -> ConceptId.of(e.at(REST.Request.COMMIT_LOG_CONCEPT_ID).asString()),
                        e -> e.at(REST.Request.COMMIT_LOG_SHARDING_COUNT).asLong()));
    }

    /**
     * Updates the type counts in redis and checks if sharding is needed.
     *
     * @param keyspace The keyspace of the graph which the type comes from
     * @param conceptId The id of the concept with counts to update
     * @param value The number of instances which the type has gained/lost
     * @return true if sharding is needed.
     */
    private static boolean updateShardCounts(String keyspace, ConceptId conceptId, long value){
        long numShards = redis.getCount(RedisConnection.getKeyNumShards(keyspace, conceptId));
        if(numShards == 0) numShards = 1;
        long numInstances = redis.adjustCount(RedisConnection.getKeyNumInstances(keyspace, conceptId), value);
        return numInstances > SHARDING_THRESHOLD * numShards;
    }

    /**
     * Performs the high level sharding operation. This includes:
     * - Acquiring a lock to ensure only one thing can shard
     * - Checking if sharding is still needed after having the lock
     * - Actually sharding
     * - Incrementing the number of shards on each type
     *
     * @param keyspace The graph containing the type to shard
     * @param conceptId The id of the concept to shard
     */
    private static void shardConcept(String keyspace, ConceptId conceptId){
        Lock engineLock = LockProvider.getLock(getLockingKey(keyspace, conceptId));
        engineLock.lock(); //Try to get the lock

        try {
            //Check if sharding is still needed. Another engine could have sharded whilst waiting for lock
            if (updateShardCounts(keyspace, conceptId, 0)) {

                //Shard
                GraphMutators.runGraphMutationWithRetry(FACTORY, keyspace, graph -> {
                    graph.admin().shard(conceptId);
                    graph.admin().commitNoLogs();
                });

                //Update number of shards
                redis.adjustCount(RedisConnection.getKeyNumShards(keyspace, conceptId), 1);
            }
        } finally {
            engineLock.unlock();
        }
    }

    private static String getLockingKey(String keyspace, ConceptId conceptId){
        return "/updating-instance-count-lock/" + keyspace + "/" + conceptId.getValue();
    }

    @Override
    public boolean resume(Consumer<TaskCheckpoint> saveCheckpoint, TaskCheckpoint lastCheckpoint) {
        throw new UnsupportedOperationException(this.getClass().getName() + " task cannot be resumed");
    }

    /**
     * Helper method which creates PP Task States.
     *
     * @param creator The class which is creating the task
     * @return The executable postprocessing task state
     */
    public static TaskState createTask(Class creator){
        return TaskState.of(UpdatingInstanceCountTask.class,
                creator.getName(),
                TaskSchedule.now(),
                TaskState.Priority.HIGH);
    }

    /**
     * Helper method which creates the task config needed in order to execute the updating count task
     *
     * @param keyspace The keyspace of the graph to execute this on.
     * @param config The config which contains the concepts with updated counts
     * @return The task configuration encapsulating the above details in a manner executable by the task runner
     */
    public static TaskConfiguration createConfig(String keyspace, String config){
        Json countingConfiguration = Json.object();
        countingConfiguration.set(REST.Request.KEYSPACE, keyspace);
        countingConfiguration.set(REST.Request.COMMIT_LOG_COUNTING, Json.read(config).at(REST.Request.COMMIT_LOG_COUNTING));;
        return TaskConfiguration.of(countingConfiguration);
    }
}