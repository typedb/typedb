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

import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.connection.RedisCountStorage;
import ai.grakn.engine.tasks.manager.TaskConfiguration;
import ai.grakn.engine.tasks.manager.TaskSchedule;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.kb.internal.GraknTxAbstract;
import ai.grakn.util.REST;
import com.codahale.metrics.Timer.Context;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;

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
    private final static Logger LOG = LoggerFactory.getLogger(UpdatingInstanceCountTask.class);

    @Override
    public boolean start() {
        final long shardingThreshold = engineConfiguration().getPropertyAsLong(GraknTxAbstract.SHARDING_THRESHOLD);
        final int maxRetry = engineConfiguration().getPropertyAsInt(GraknEngineConfig.LOADER_REPEAT_COMMITS);
        try (Context context = metricRegistry()
                .timer(name(UpdatingInstanceCountTask.class, "execution")).time()) {
            Map<ConceptId, Long> jobs = getCountUpdatingJobs(configuration());
            metricRegistry().histogram(name(UpdatingInstanceCountTask.class, "jobs"))
                    .update(jobs.size());
            Keyspace keyspace = Keyspace.of(configuration().json().at(REST.Request.KEYSPACE).asString());

            //We Use redis to keep track of counts in order to ensure sharding happens in a centralised manner.
            //The graph cannot be used because each engine can have it's own snapshot of the graph with caching which makes
            //values only approximately correct
            Set<ConceptId> conceptToShard = new HashSet<>();

            //Update counts
            jobs.forEach((key, value) -> {
                metricRegistry()
                        .histogram(name(UpdatingInstanceCountTask.class, "shard-size-increase"))
                        .update(value);
                Context contextSingle = metricRegistry()
                        .timer(name(UpdatingInstanceCountTask.class, "execution-single")).time();
                try {
                    if (updateShardCounts(redis(), keyspace, key, value, shardingThreshold)) {
                        conceptToShard.add(key);
                    }
                } finally {
                    contextSingle.stop();
                }
            });

            //Shard anything which requires sharding
            conceptToShard.forEach(type -> {
                Context contextSharding = metricRegistry().timer("sharding").time();
                try {
                    shardConcept(redis(), factory(), keyspace, type, maxRetry, shardingThreshold);
                } finally {
                    contextSharding.stop();
                }
            });
            LOG.debug("Updating instance count successful for {} tasks", jobs.size());
            return true;
        } catch(Exception e) {
            LOG.error("Could not terminate task", e);
            throw e;
        }
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
    private static boolean updateShardCounts(
            RedisCountStorage redis, Keyspace keyspace, ConceptId conceptId, long value, long shardingThreshold){
        long numShards = redis.getCount(RedisCountStorage.getKeyNumShards(keyspace, conceptId));
        if(numShards == 0) numShards = 1;
        long numInstances = redis.adjustCount(
                RedisCountStorage.getKeyNumInstances(keyspace, conceptId), value);
        return numInstances > shardingThreshold * numShards;
    }

    /**
     * Performs the high level sharding operation. This includes:
     * - Acquiring a lock to ensure only one thing can shard
     * - Checking if sharding is still needed after having the lock
     * - Actually sharding
     * - Incrementing the number of shards on each type
     *
     * @param keyspace The database containing the {@link ai.grakn.concept.Type} to shard
     * @param conceptId The id of the concept to shard
     */
    private void shardConcept(RedisCountStorage redis, EngineGraknTxFactory factory,
            Keyspace keyspace, ConceptId conceptId, int maxRetry, long shardingThreshold){
        Lock engineLock = this.getLockProvider().getLock(getLockingKey(keyspace, conceptId));
        engineLock.lock(); //Try to get the lock

        try {
            //Check if sharding is still needed. Another engine could have sharded whilst waiting for lock
            if (updateShardCounts(redis, keyspace, conceptId, 0, shardingThreshold)) {

                //Shard
                GraknTxMutators.runMutationWithRetry(factory, keyspace, maxRetry, graph -> {
                    graph.admin().shard(conceptId);
                    graph.admin().commitNoLogs();
                });

                //Update number of shards
                redis.adjustCount(RedisCountStorage.getKeyNumShards(keyspace, conceptId), 1);
            }
        } finally {
            engineLock.unlock();
        }
    }

    private static String getLockingKey(Keyspace keyspace, ConceptId conceptId){
        return "/updating-instance-count-lock/" + keyspace + "/" + conceptId.getValue();
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
    public static TaskConfiguration createConfig(Keyspace keyspace, String config){
        Json countingConfiguration = Json.object();
        countingConfiguration.set(REST.Request.KEYSPACE, keyspace.getValue());
        countingConfiguration.set(REST.Request.COMMIT_LOG_COUNTING, Json.read(config).at(REST.Request.COMMIT_LOG_COUNTING));
        return TaskConfiguration.of(countingConfiguration);
    }
}