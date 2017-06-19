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

package ai.grakn.engine.tasks.manager;

import ai.grakn.engine.TaskId;
import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.util.EngineID;
import ai.grakn.exception.GraknBackendException;
import java.util.Base64;
import java.util.Set;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang.SerializationUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;


/**
 * DAO for redis task states
 *
 * @author Domenico Corapi
 */
public class RedisTaskStorage implements TaskStateStorage {

    private JedisPool redis;

    private RedisTaskStorage(JedisPool redis) {
        this.redis = redis;
    }

    public static RedisTaskStorage create(JedisPool jedisPool) {
        return new RedisTaskStorage(jedisPool);
    }

    @Override
    public TaskId newState(TaskState state) throws GraknBackendException {
        return null;
    }

    @Override
    public Boolean updateState(TaskState state) {
        try(Jedis jedis = redis.getResource()){
            // TODO find a better way to represent the state
            String status = jedis.set(state.getId().getValue(), new String(Base64.getEncoder().encode(SerializationUtils.serialize(state)),
                    Charsets.UTF_8));
            // TODO not sure
            return status.equalsIgnoreCase("OK");
        }
    }

    @Override
    public TaskState getState(TaskId id) throws GraknBackendException {
        try(Jedis jedis = redis.getResource()){
            String value = jedis.get(id.getValue());
            return (TaskState) org.apache.commons.lang.SerializationUtils.deserialize(Base64.getDecoder().decode(value));
        }
    }

    @Override
    public boolean containsTask(TaskId id) {
        try(Jedis jedis = redis.getResource()){
            String value = jedis.get(id.getValue());
            return value != null;
        }
    }

    @Override
    public Set<TaskState> getTasks(TaskStatus taskStatus, String taskClassName, String createdBy,
            EngineID runningOnEngine, int limit, int offset) {
        return null;
    }
}
