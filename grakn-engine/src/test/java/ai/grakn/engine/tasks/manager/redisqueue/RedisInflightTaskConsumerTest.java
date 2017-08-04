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

package ai.grakn.engine.tasks.manager.redisqueue;

import static ai.grakn.engine.tasks.manager.redisqueue.RedisInflightTaskConsumer.ITERATIONS;
import com.codahale.metrics.MetricRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import net.greghaines.jesque.ConfigBuilder;
import org.junit.Test;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

public class RedisInflightTaskConsumerTest {

    private static final String QUEUE_NAME = "QUEUE_NAME";
    public static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();

    private static String TRICKY_TASK = "{\"class\":\"ai.grakn.engine.tasks.manager.redisqueue.Task\",\"args\":[{\"taskState\":{\"creator\":\"\",\"schedule\":{\"runAt\":1499800245809,\"interval\":null},\"id\":{\"value\":\"0debb87c-a4cd-46d2-90a7-03efbb10318f\"},\"priority\":\"LOW\",\"status\":\"CREATED\",\"taskClassName\":\"ai.grakn.engine.tasks.mock.LongExecutionMockTask\"},\"taskConfiguration\":{\"configuration\":\"{\\\"id\\\":\\\"0cc42a2d-134c-4385-a4e9-69edcc10fee8\\\"}\"}}],\"vars\":null}";

    @Test
    public void whenRunningInFlightConsumerWithTrickyTask_NoFailures()
            throws ExecutionException, InterruptedException, TimeoutException {
        JedisPool jedisPool = mock(JedisPool.class);
        Jedis jedis = mock(Jedis.class);
        when(jedisPool.getResource()).thenReturn(jedis);
        HashSet<String> stringHashSet = new HashSet<>();
        String k = "resque:something:something";
        stringHashSet.add(k);
        when(jedis.keys(anyString())).thenReturn(stringHashSet);
        ArrayList<String> v = new ArrayList<>();
        v.add(TRICKY_TASK);
        when(jedis.lrange(anyString(), anyLong(), anyLong())).thenReturn(v);
        when(jedis.watch(anyString())).thenReturn("");
        Transaction transaction = mock(Transaction.class);
        when(transaction.rpoplpush(anyString(), anyString())).thenReturn(new Response<>(
                BuilderFactory.STRING));
        when(jedis.multi()).thenReturn(transaction);
        RedisInflightTaskConsumer redisInflightTaskConsumer =
                new RedisInflightTaskConsumer(jedisPool, Duration.ofSeconds(30),
                        new ConfigBuilder().build(), QUEUE_NAME, METRIC_REGISTRY);
        redisInflightTaskConsumer.run();
        verify(transaction, times( ITERATIONS)).rpoplpush(anyString(), anyString());
    }
}