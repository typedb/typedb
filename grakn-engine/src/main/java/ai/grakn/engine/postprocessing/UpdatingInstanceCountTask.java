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

import ai.grakn.concept.TypeLabel;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.postprocessing.util.TaskConfigReader;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.engine.tasks.TaskConfiguration;
import ai.grakn.engine.tasks.connection.RedisConnection;
import ai.grakn.graph.internal.AbstractGraknGraph;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

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
public class UpdatingInstanceCountTask implements BackgroundTask {
    public static final RedisConnection redis = RedisConnection.getConnection();
    private static final long SHARDING_THRESHOLD = GraknEngineConfig.getInstance().getPropertyAsLong(AbstractGraknGraph.SHARDING_THRESHOLD);

    @Override
    public boolean start(Consumer<TaskCheckpoint> saveCheckpoint, TaskConfiguration configuration) {
        Map<TypeLabel, Long> jobs = TaskConfigReader.getCountUpdatingJobs(configuration);
        String keyspace = TaskConfigReader.getKeyspace(configuration);

        //We Use redis to keep track of counts in order to ensure sharding happens in a centralised manner.
        //The graph cannot be used because each engine can have it's own snapshot of the graph with caching which makes
        //values only approximately correct
        Set<TypeLabel> typesToShard = new HashSet<>();

        //Update counts
        jobs.forEach((key, value) -> {
            if(updateTypeCounts(keyspace, key, value)) typesToShard.add(key);
        });

        //Shard anything which requires sharding
        typesToShard.forEach(type -> shardType(keyspace, type));

        return true;
    }

    /**
     * Updates the type counts in redis and checks if sharding is needed.
     *
     * @param keyspace The keyspace of the graph which the type comes from
     * @param label The label of the type with counts to update
     * @param value The number of instances which the type has gained/lost
     * @return true if sharding is needed.
     */
    private static boolean updateTypeCounts(String keyspace, TypeLabel label, long value){
        long numShards = redis.getCount(RedisConnection.getKeyNumShards(keyspace, label));
        if(numShards == 0) numShards = 1;
        long numInstances = redis.adjustCount(RedisConnection.getKeyNumInstances(keyspace, label), value);
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
     * @param label The label of the type to shard
     */
    private static void shardType(String keyspace, TypeLabel label){
        Lock engineLock = LockProvider.getLock(getLockingKey());
        engineLock.lock(); //Try to get the lock

        //Check if sharding is still needed. Another engine could have sharded whilst waiting for lock
        if(updateTypeCounts(keyspace, label, 0)) {

            //Shard
            GraphMutators.runGraphMutationWithRetry(keyspace, graph -> {
                graph.admin().shard(label);
                graph.admin().commitNoLogs();
            });

            //Update number of shards
            redis.adjustCount(RedisConnection.getKeyNumShards(keyspace, label), 1);
        }

        engineLock.unlock();
    }
    //TODO: Add parameters keyspace and label to locking
    public static String getLockingKey(){
        return "/updating-instance-count-lock";
    }

    @Override
    public boolean stop() {
        throw new UnsupportedOperationException(this.getClass().getName() + " task cannot be stopped while in progress");
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException(this.getClass().getName() + " task cannot be paused while in progress");
    }

    @Override
    public boolean resume(Consumer<TaskCheckpoint> saveCheckpoint, TaskCheckpoint lastCheckpoint) {
        throw new UnsupportedOperationException(this.getClass().getName() + " task cannot be resumed");
    }
}