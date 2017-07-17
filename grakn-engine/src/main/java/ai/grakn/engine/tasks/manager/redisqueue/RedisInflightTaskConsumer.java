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

package ai.grakn.engine.tasks.manager.redisqueue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.TimerTask;
import mjson.Json;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.utils.ResqueConstants;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.util.Pool;

/**
 * Consumer from a redis queue
 *
 * @author Domenico Corapi
 */
public class RedisInflightTaskConsumer extends TimerTask {
    private static final Logger LOG = LoggerFactory.getLogger(RedisInflightTaskConsumer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static final int ITERATIONS = 10;

    private Pool<Jedis> jedisPool;
    private Duration processInterval;
    private Config config;
    private String queueName;

    public RedisInflightTaskConsumer(Pool<Jedis> jedisPool, Duration processInterval,
            Config config, String queueName) {
        this.jedisPool = jedisPool;
        this.processInterval = processInterval;
        this.config = config;
        this.queueName = queueName;
    }

    @Override
    public void run() {
        try(Jedis resource = jedisPool.getResource()) {
            Set<String> keys = resource
                    .keys(String.format("resque:%s:*", ResqueConstants.INFLIGHT));
            for(String key : keys) {
                for(int i = 0; i < ITERATIONS; i++) {
                    LOG.debug("Processing inflight for {}, iteration {}", key, i);
                    resource.watch(key);
                    List<String> elements = resource.lrange(key, -1, -1);
                    LOG.info("NOOP");
                    // Optimistic concurrency control. If there are changes in keys we fail
                    if (!elements.isEmpty()) {
                        String head = elements.get(0);
                        try {
                            Job job = objectMapper.readValue(head, Job.class);
                            if (job.getArgs().length > 0) {
                                processElement(key, resource, head);
                            }
                        } catch (IOException e) {
                            LOG.error("Could not deserialize task, moving to head: {}", head, e);
                            // Moving to the head
                            attemptMove(resource, key, key);
                        }
                    }
                }
            }
        }
    }

    private void processElement(String key, Jedis resource, String head) {
        // TODO Use Jackson for this
        long runAt = Json.read(head).at("args").at(0).at("taskState")
                .at("schedule").at("runAt").asLong();
        Instant runAtDate = Instant.ofEpochMilli(runAt);
        Duration gap = Duration.between(runAtDate, Instant.now());
        if (gap.getSeconds() > processInterval.getSeconds()) {
            LOG.info("Found dead task in inflight: {}", head);
            String keyDest = JesqueUtils
                    .createKey(config.getNamespace(), QUEUE, queueName);
            attemptMove(resource, key, keyDest);
        }
    }

    private void attemptMove(Jedis resource, String key, String keyDest) {
        Transaction transaction = resource.multi();
        transaction.rpoplpush(key, keyDest);
        List<Object> result = transaction.exec();
        if (result == null) {
            LOG.warn("Could not move job from {} to {}, something modified the queue. "
                    + "The move will be retried when the task is next scheduled", key, keyDest);
        }
    }
}
