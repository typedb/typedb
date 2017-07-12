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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import net.greghaines.jesque.ConfigBuilder;
import org.junit.Test;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisInflightTaskConsumerTest {

    private static final String QUEUE_NAME = "QUEUE_NAME";

    private static String TRICKY_TASK = "{\"class\":\"ai.grakn.engine.tasks.manager.redisqueue.Task\",\"args\":[{\"taskState\":{\"creator\":\"\",\"schedule\":{\"runAt\":1499800245809,\"interval\":null},\"id\":{\"value\":\"0debb87c-a4cd-46d2-90a7-03efbb10318f\"},\"priority\":\"LOW\",\"status\":\"CREATED\",\"taskClassName\":\"ai.grakn.engine.tasks.mock.LongExecutionMockTask\"},\"taskConfiguration\":{\"configuration\":\"{\\\"id\\\":\\\"0cc42a2d-134c-4385-a4e9-69edcc10fee8\\\"}\"}}],\"vars\":null}";

    @Test
    public void whenRunningInFlightConsumerWithTrickyTask_NoFailures() throws ExecutionException, InterruptedException {
        JedisPool jedisPool = mock(JedisPool.class);
        Jedis jedis = mock(Jedis.class);
        when(jedisPool.getResource()).thenReturn(jedis);
        HashSet<String> stringHashSet = new HashSet<>();
        String k = "resque:something:something";
        stringHashSet.add(k);
        when(jedis.keys(anyString())).thenReturn(stringHashSet);
        ArrayList<String> v = new ArrayList<>();
        v.add(TRICKY_TASK);
        when(jedis.lrange(eq(k), anyInt(), anyInt())).thenReturn(v);
        when(jedis.rpoplpush(anyString(), anyString())).thenReturn("does not matter");
        RedisInflightTaskConsumer redisInflightTaskConsumer =
                new RedisInflightTaskConsumer(jedisPool,
                        Duration.ofSeconds(2), new ConfigBuilder().build(), QUEUE_NAME);
        redisInflightTaskConsumer.run();
    }
}