/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.engine.task.postprocessing.redisstorage;

import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.task.postprocessing.IndexStorage;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.util.Pool;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 *     Stores a list of indices and vertex ids representing those indices which need to be post processed
 * </p>
 *
 * @author fppt
 */
public class RedisIndexStorage implements IndexStorage {
    private final RedisStorage redisStorage;

    private RedisIndexStorage(Pool<Jedis> jedisPool, MetricRegistry metricRegistry) {
        redisStorage = new RedisStorage(jedisPool, metricRegistry);
    }

    public static RedisIndexStorage create(Pool<Jedis> jedisPool, MetricRegistry metricRegistry) {
        return new RedisIndexStorage(jedisPool, metricRegistry);
    }

    @Override
    public void addIndex(Keyspace keyspace, String index, Set<ConceptId> conceptIds){
        String listOfIndicesKey = getIndicesKey(keyspace);
        String listOfIdsKey = getConceptIdsKey(keyspace, index);

        redisStorage.contactRedis(jedis -> {
            //Track all the indices which need to be post proceed
            jedis.sadd(listOfIndicesKey, index);
            conceptIds.forEach(id -> jedis.sadd(listOfIdsKey, id.getValue()));
            return null;
        });
    }

    @Override
    @Nullable
    public String popIndex(Keyspace keyspace){
        String indexKey = getIndicesKey(keyspace);
        return redisStorage.contactRedis(jedis -> jedis.spop(indexKey));
    }

    @Override
    public Set<ConceptId> popIds(Keyspace keyspace, String index){
        String idKey = getConceptIdsKey(keyspace, index);
        return redisStorage.contactRedis(jedis -> {
            Transaction tx = jedis.multi();
            Response<Set<String>> responseIds = tx.smembers(idKey);
            tx.del(idKey);
            tx.exec();
            return  responseIds.get().stream().map(ConceptId::of).collect(Collectors.toSet());
        });
    }

    /**
     * The key which refers to  a list of all the indices in a certain {@link Keyspace} which need to be post processed
     */
    @VisibleForTesting
    public static String getIndicesKey(Keyspace keyspace){
        return "IndicesToProcess_" + keyspace.getValue();
    }

    /**
     * The key which refers to a set of vertices currently pointing to the same index
     */
    @VisibleForTesting
    public static String getConceptIdsKey(Keyspace keyspace, String index){
        return "IdsToPostProcess_" + keyspace.getValue() + "_Id_" + index;
    }
}
