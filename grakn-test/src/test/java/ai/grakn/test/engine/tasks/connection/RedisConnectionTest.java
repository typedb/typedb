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

package ai.grakn.test.engine.tasks.connection;

import ai.grakn.concept.TypeLabel;
import ai.grakn.engine.tasks.connection.RedisConnection;
import ai.grakn.test.EngineContext;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
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
public class RedisConnectionTest {

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();
    private static RedisConnection redis;

    @BeforeClass
    public static void getConnection(){
        redis = RedisConnection.getConnection();
    }

    @Test
    public void whenIncreasingCountOnRedisConcurrently_EnsureAllThreadCountsArePersisted() throws ExecutionException, InterruptedException {
        String keyspace = UUID.randomUUID().toString();
        TypeLabel label = TypeLabel.of("Roach");
        int[] counts = {5, 5, 10, 10, -8, -2, 5, 5, -7};
        ExecutorService pool = Executors.newCachedThreadPool();
        Set<Future> futures = new HashSet<>();

        assertEquals(0, redis.adjustCount(keyspace, label, 0));

        for(int i =0; i < counts.length; i ++) {
            int finalI = i;
            futures.add(pool.submit(() -> redis.adjustCount(keyspace, label, counts[finalI])));
        }
        for (Future future : futures) {
            future.get();
        }

        assertEquals(23, redis.adjustCount(keyspace, label, 0));
    }

}
