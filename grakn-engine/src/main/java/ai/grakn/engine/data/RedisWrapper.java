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
package ai.grakn.engine.data;

import ai.grakn.engine.util.SimpleURI;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.util.Pool;

/**
 * This class just wraps a Jedis and Redisson pool so it's transparent whether
 * we use Sentinel or not (and TODO partitioning)
 *
 * It also keeps Jedis and Redisson in the same place. We are using both because jedis is
 * supported by Jesque, while Redisson supports distributed locks.
 *
 * @author pluraliseseverythings
 */
public class RedisWrapper {

    private Pool<Jedis> jedisPool;
    private RedissonClient redissonClient;

    private RedisWrapper(Pool<Jedis> jedisPool, RedissonClient redissonClient) {
        this.jedisPool = jedisPool;
        this.redissonClient = redissonClient;
    }

    public Pool<Jedis> getJedisPool() {
        return jedisPool;
    }

    public RedissonClient getRedissonClient() {
        return redissonClient;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for the wrapper
     */
    public static class Builder {
        private boolean useSentinel = false;
        private Set<String> uriSet = new HashSet<>();
        private String masterName = null;

        public Builder setUseSentinel(boolean useSentinel) {
            this.useSentinel = useSentinel;
            return this;
        }

        public Builder addURI(String uri) {
            this.uriSet.add(uri);
            return this;
        }

        public Builder setURI(Collection<String> uri) {
            this.uriSet = new HashSet<>(uri);
            return this;
        }

        public Builder setMasterName(String masterName) {
            this.masterName = masterName;
            return this;
        }

        public RedisWrapper build() {
            // TODO make connection pool sizes configurable
            Preconditions.checkState(!uriSet.isEmpty(), "Trying to build RedisWrapper without uriSet");
            Preconditions.checkState(!(!useSentinel && uriSet.size() > 1), "More than one URL provided but Sentinel not used");
            Preconditions.checkState(!(useSentinel && masterName == null), "Using Sentinel but master name not provided");
            Pool<Jedis> jedisPool;
            Config redissonConfig = new Config();
            if (useSentinel) {
                jedisPool = new JedisSentinelPool(masterName, uriSet);
                redissonConfig.useSentinelServers()
                        .setMasterConnectionPoolSize(3)
                        .setSlaveConnectionPoolSize(3)
                        .addSentinelAddress(uriSet.toArray(new String[uriSet.size()]));
            } else {
                String uri = uriSet.stream().findFirst().get();
                SimpleURI simpleURI = new SimpleURI(uri);
                jedisPool = new JedisPool(simpleURI.getHost(), simpleURI.getPort());
                redissonConfig.useSingleServer()
                        // TODO make connection pool configurable
                        .setConnectionPoolSize(5)
                        .setAddress(uri);
            }
            return new RedisWrapper(jedisPool, Redisson.create(redissonConfig));
        }
    }

}
