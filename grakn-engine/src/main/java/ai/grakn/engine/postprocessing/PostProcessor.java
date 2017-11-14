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

import ai.grakn.GraknConfigKey;
import ai.grakn.GraknTx;
import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * <p>
 *     Class responsible for post processing concepts which may be broken.
 * </p>
 *
 * <p>
 *     Contains the logic which is needed for post processing {@link ai.grakn.concept.Concept}s for the moment this
 *     includes:
 *
 *     - Merging duplicate {@link ai.grakn.concept.Attribute}s
 *     - Counting new {@link ai.grakn.concept.Thing}s which have been created under {@link ai.grakn.concept.Type}s
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public class PostProcessor {
    private final static Logger LOG = LoggerFactory.getLogger(PostProcessor.class);
    private final GraknEngineConfig engineConfig;
    private final MetricRegistry metricRegistry;
    private final LockProvider lockProvider;
    private final RedisCountStorage redis;
    private final EngineGraknTxFactory factory;

    @Deprecated
    private static final String LOCK_KEY = "/post-processing-lock";

    private PostProcessor(GraknEngineConfig engineConfig, Pool<Jedis> jedisPool, EngineGraknTxFactory factory, LockProvider lockProvider, MetricRegistry metricRegistry){
        this.engineConfig = engineConfig;
        this.metricRegistry = metricRegistry;
        this.lockProvider = lockProvider;
        this.redis = RedisCountStorage.create(jedisPool, metricRegistry);
        this.factory = factory;
    }

    public static PostProcessor create(GraknEngineConfig engineConfig, Pool<Jedis> jedisPool, EngineGraknTxFactory factory, LockProvider lockProvider, MetricRegistry metricRegistry){
        return new PostProcessor(engineConfig, jedisPool, factory, lockProvider, metricRegistry);
    }

    /**
     * Updates the counts of {@link ai.grakn.concept.Type}s based on the commit logs received.
     *
     * @param keyspace The {@link Keyspace} which contains {@link ai.grakn.concept.Type}s with new instances.
     * @param commitLog The commit log containing the details of the job
     */
    public void updateCounts(Keyspace keyspace, Json commitLog){
        final long shardingThreshold = engineConfig.getProperty(GraknConfigKey.SHARDING_THRESHOLD);
        final int maxRetry = engineConfig.getProperty(GraknConfigKey.LOADER_REPEAT_COMMITS);
        try (Timer.Context context = metricRegistry.timer(name(PostProcessor.class, "execution")).time()) {
            Map<ConceptId, Long> jobs = getCountUpdatingJobs(commitLog);
            metricRegistry.histogram(name(PostProcessor.class, "jobs"))
                    .update(jobs.size());

            //We Use redis to keep track of counts in order to ensure sharding happens in a centralised manner.
            //The graph cannot be used because each engine can have it's own snapshot of the graph with caching which makes
            //values only approximately correct
            Set<ConceptId> conceptToShard = new HashSet<>();

            //Update counts
            jobs.forEach((key, value) -> {
                metricRegistry
                        .histogram(name(PostProcessor.class, "shard-size-increase"))
                        .update(value);
                Timer.Context contextSingle = metricRegistry
                        .timer(name(PostProcessor.class, "execution-single")).time();
                try {
                    if (updateShardCounts(redis, keyspace, key, value, shardingThreshold)) {
                        conceptToShard.add(key);
                    }
                } finally {
                    contextSingle.stop();
                }
            });

            //Shard anything which requires sharding
            conceptToShard.forEach(type -> {
                Timer.Context contextSharding = metricRegistry.timer("sharding").time();
                try {
                    shardConcept(redis, factory, keyspace, type, maxRetry, shardingThreshold);
                } finally {
                    contextSharding.stop();
                }
            });
            LOG.debug("Updating instance count successful for {} tasks", jobs.size());
        } catch(Exception e) {
            LOG.error("Could not terminate task", e);
            throw e;
        }
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
        Lock engineLock = lockProvider.getLock(getLockingKey(keyspace, conceptId));
        engineLock.lock(); //Try to get the lock

        try {
            //Check if sharding is still needed. Another engine could have sharded whilst waiting for lock
            if (updateShardCounts(redis, keyspace, conceptId, 0, shardingThreshold)) {

                //Shard
                GraknTxMutators.runMutationWithRetry(factory, keyspace, maxRetry, graph -> {
                    graph.admin().shard(conceptId);
                    graph.admin().commitSubmitNoLogs();
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
     * Extracts the type labels and count from the Json configuration
     * @param json The configuration which contains types counts
     * @return A map indicating the number of instances each type has gained or lost
     */
    private static Map<ConceptId, Long> getCountUpdatingJobs(Json json){
        return  json.at(REST.Request.COMMIT_LOG_COUNTING).asJsonList().stream()
                .collect(Collectors.toMap(
                        e -> ConceptId.of(e.at(REST.Request.COMMIT_LOG_CONCEPT_ID).asString()),
                        e -> e.at(REST.Request.COMMIT_LOG_SHARDING_COUNT).asLong()));
    }

    /**
     * Merges duplicate {@link ai.grakn.concept.Concept}s based on the unique index provided plus the {@link ConceptId}s
     * of the suspected duplicates
     *
     * @param tx The {@link GraknTx} responsible for performing the merge
     * @param conceptIndex The unique {@link ai.grakn.concept.Concept} index which is supposed to exist only once
     *                     across the entire DB.
     * @param conceptIds The {@link ConceptId}s of the suspected duplicates
     */
    public void mergeDuplicateConcepts(GraknTx tx, String conceptIndex, Set<ConceptId> conceptIds){
        Preconditions.checkNotNull(lockProvider, "Lock provider was null, possible race condition in initialisation");
        if(tx.admin().duplicateResourcesExist(conceptIndex, conceptIds)){

            // Acquire a lock when you post process on an index to prevent race conditions
            // Lock is acquired after checking for duplicates to reduce runtime
            Lock indexLock = lockProvider.getLock(LOCK_KEY + "/" + conceptIndex);
            indexLock.lock();

            try {
                // execute the provided post processing method
                boolean commitNeeded = tx.admin().fixDuplicateResources(conceptIndex, conceptIds);

                // ensure post processing was correctly executed
                if(commitNeeded) {
                    validateMerged(tx, conceptIndex, conceptIds).
                            ifPresent(message -> {
                                throw new RuntimeException(message);
                            });

                    // persist merged concepts
                    tx.admin().commitSubmitNoLogs();
                }
            } finally {
                indexLock.unlock();
            }
        }
    }

    /**
     * Checks that post processing was done successfully by doing two things:
     *  1. That there is only 1 valid conceptID left
     *  2. That the concept Index does not return null
     * @param graph A grakn graph to run the checks against.
     * @param conceptIndex The concept index which MUST return a valid concept
     * @param conceptIds The concpet ids which should only return 1 valid concept
     * @return An error if one of the above rules are not satisfied.
     */
    private Optional<String> validateMerged(GraknTx graph, String conceptIndex, Set<ConceptId> conceptIds){
        //Check number of valid concept Ids
        int numConceptFound = 0;
        for (ConceptId conceptId : conceptIds) {
            if (graph.getConcept(conceptId) != null) {
                numConceptFound++;
                if (numConceptFound > 1) {
                    StringBuilder conceptIdValues = new StringBuilder();
                    for (ConceptId id : conceptIds) {
                        conceptIdValues.append(id.getValue()).append(",");
                    }
                    return Optional.of("Not all concept were merged. The set of concepts [" + conceptIds.size() + "] with IDs [" + conceptIdValues.toString() + "] matched more than one concept");
                }
            }
        }

        //Check index
        if(graph.admin().getConcept(Schema.VertexProperty.INDEX, conceptIndex) == null){
            return Optional.of("The concept index [" + conceptIndex + "] did not return any concept");
        }

        return Optional.empty();
    }
}
