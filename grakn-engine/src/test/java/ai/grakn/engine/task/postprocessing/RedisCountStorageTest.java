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

package ai.grakn.engine.task.postprocessing;

import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.test.rule.InMemoryRedisContext;
import ai.grakn.util.SampleKBLoader;
import com.codahale.metrics.MetricRegistry;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

/**
 * <p>
 *    Tests that the connection to redis is working and that data can be persisted and retrieved into/from redis.
 * </p>
 *
 * @author fppt
 */
public class RedisCountStorageTest {

    @ClassRule
    public static final InMemoryRedisContext IN_MEMORY_REDIS_CONTEXT = InMemoryRedisContext.create();

    private static RedisCountStorage redis;

    @BeforeClass
    public static void getConnection(){
        redis = RedisCountStorage.create(IN_MEMORY_REDIS_CONTEXT.jedisPool(), new MetricRegistry());
    }

    @Test
    public void whenIncreasingCountOnRedisConcurrently_EnsureAllThreadCountsArePersisted() throws ExecutionException, InterruptedException {
        Keyspace keyspace = SampleKBLoader.randomKeyspace();
        ConceptId conceptId = ConceptId.of("Roach");
        int[] counts = {5, 5, 10, 10, -8, -2, 5, 5, -7};
        ExecutorService pool = Executors.newCachedThreadPool();
        Set<Future> futures = new HashSet<>();

        assertEquals(0, redis.getCount(RedisCountStorage.getKeyNumInstances(keyspace, conceptId)));

        for(int i =0; i < counts.length; i ++) {
            int finalI = i;
            futures.add(pool.submit(() -> redis.incrementCount(
                    RedisCountStorage.getKeyNumInstances(keyspace, conceptId), counts[finalI])));
        }
        for (Future future : futures) {
            future.get();
        }

        assertEquals(23, redis.getCount(RedisCountStorage.getKeyNumInstances(keyspace, conceptId)));
    }

    @Test
    public void whenChangingCountsOnRedis_EnsureValueIsChanges(){
        Keyspace keyspace1 = SampleKBLoader.randomKeyspace();
        Keyspace keyspace2 = SampleKBLoader.randomKeyspace();
        ConceptId roach = ConceptId.of("Roach");
        ConceptId ciri = ConceptId.of("Ciri");

        assertEquals(0, redis.getCount(RedisCountStorage.getKeyNumInstances(keyspace1, roach)));
        assertEquals(0, redis.getCount(RedisCountStorage.getKeyNumInstances(keyspace2, roach)));

        redis.incrementCount(RedisCountStorage.getKeyNumInstances(keyspace1, roach), 1);
        assertEquals(1, redis.getCount(RedisCountStorage.getKeyNumInstances(keyspace1, roach)));
        assertEquals(0, redis.getCount(RedisCountStorage.getKeyNumInstances(keyspace2, roach)));

        redis.incrementCount(RedisCountStorage.getKeyNumInstances(keyspace2, ciri), 1);
        assertEquals(0, redis.getCount(RedisCountStorage.getKeyNumInstances(keyspace1, ciri)));
        assertEquals(1, redis.getCount(RedisCountStorage.getKeyNumInstances(keyspace2, ciri)));
    }
}
