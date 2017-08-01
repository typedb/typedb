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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.util.Pool;

/**
 * This class just wraps a Jedis  pool so it's transparent whether
 * we use Sentinel or not (and TODO partitioning)
 *
 * @author pluraliseseverythings
 */
public class RedisWrapper {

    private Pool<Jedis> jedisPool;

    private RedisWrapper(Pool<Jedis> jedisPool) {
        this.jedisPool = jedisPool;
    }

    public Pool<Jedis> getJedisPool() {
        return jedisPool;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for the wrapper
     */
    public static class Builder {

        public static final int DEFAULT_PORT = 6379;
        public static final int TIMEOUT = 5000;

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
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setTestOnBorrow(true);
            poolConfig.setTestOnReturn(true);
            if (useSentinel) {
                jedisPool = new JedisSentinelPool(masterName, uriSet, poolConfig, TIMEOUT);
            } else {
                String uri = uriSet.stream().findFirst().get();
                SimpleURI simpleURI = SimpleURI.withDefaultPort(uri, DEFAULT_PORT);
                jedisPool = new JedisPool(poolConfig, simpleURI.getHost(), simpleURI.getPort(), TIMEOUT);
            }
            return new RedisWrapper(jedisPool);
        }
    }

}
