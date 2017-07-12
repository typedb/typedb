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
import net.greghaines.jesque.Config;
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.utils.ResqueConstants;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

/**
 * Consumer from a redis queue
 *
 * @author Domenico Corapi
 */
public class RedisInflightTaskConsumer extends TimerTask {
    private final static Logger LOG = LoggerFactory.getLogger(RedisInflightTaskConsumer.class);

    private final static ObjectMapper objectMapper = new ObjectMapper();

    public static final int END = 10;
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
                LOG.debug("Processing inflight for {}", key);
                List<String> elements = resource.lrange(key, 0, 0);
                if (!elements.isEmpty()) {
                    String head = elements.get(0);
                    try {
                        Task task = objectMapper.readValue(head, Task.class);
                        long runAt = task.getTaskState().schedule().getRunAt();
                        Instant runAtDate = Instant.ofEpochMilli(runAt);
                        Duration gap = Duration.between(runAtDate, Instant.now());
                        if (gap.getSeconds() > processInterval.getSeconds()) {
                            LOG.info("Found dead task in inflight: ", head);
                            resource.rpoplpush(key, JesqueUtils.createKey(config.getNamespace(), QUEUE, queueName));
                        }
                    } catch (IOException e) {
                        LOG.error("Could not deserialize task, process manually from inflight queue: {}", head, e);
                    }
                }
            }
        }
    }
}
