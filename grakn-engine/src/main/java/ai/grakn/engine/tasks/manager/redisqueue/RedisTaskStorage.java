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

import ai.grakn.engine.TaskId;
import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.engine.tasks.manager.TaskStateStorage;
import ai.grakn.engine.util.EngineID;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.redisq.Redisq;
import ai.grakn.redisq.State;
import ai.grakn.redisq.StateInfo;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import static com.codahale.metrics.MetricRegistry.name;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * DAO for redis task states
 *
 * @author Domenico Corapi
 */
public class RedisTaskStorage implements TaskStateStorage {

    private static final Logger LOG = LoggerFactory.getLogger(RedisTaskStorage.class);
    private final EngineID engineID;
    private final Meter writeError;

    private Redisq<Task> redis;

    private RedisTaskStorage(Redisq<Task> redis, EngineID engineID, MetricRegistry metricRegistry) {
        this.redis = redis;
        this.engineID = engineID;
        this.writeError = metricRegistry.meter(name(RedisTaskStorage.class, "write", "error"));
    }

    public static RedisTaskStorage create(Redisq<Task> redisq, EngineID engineID, MetricRegistry metricRegistry) {
        return new RedisTaskStorage(redisq, engineID, metricRegistry);
    }

    public static RedisTaskStorage create(Redisq<Task> redisq, MetricRegistry metricRegistry) {
        return new RedisTaskStorage(redisq, EngineID.me(), metricRegistry);
    }

    private State mapStatus(TaskStatus status) {
        switch (status) {
            case CREATED:
                return State.NEW;
            case SCHEDULED:
                return State.NEW;
            case RUNNING:
                return State.PROCESSING;
            case COMPLETED:
                return State.DONE;
            case STOPPED:
                return State.NEW;
            case FAILED:
                return State.FAILED;
            default:
                return State.NEW;
        }
    }

    @Override
    public TaskId newState(TaskState state) throws GraknBackendException {
        updateState(state);
        return state.getId();
    }


    @Override
    public Boolean updateState(TaskState state) {
        try {
            redis.setState(state.getId().getValue(), mapStatus(state.status()));
            return true;
        } catch (RuntimeException e) {
            writeError.mark();
            LOG.error("Could not update state", e);
            return false;
        }
    }

    @Override
    @Nullable
    public TaskState getState(TaskId id) throws GraknBackendException {
        // TODO this is a temporary wrap
        Optional<StateInfo> state = redis.getState(id.getValue());
        if (!state.isPresent()) {
            // TODO return optional
            throw GraknBackendException.stateStorage();
        }
        TaskState ts = TaskState.of(id);
        switch(state.get().getState()) {
            case NEW:
                break;
            case FAILED:
                // TODO just a generic exception here. Consider adding this in Redisq.
                ts.markFailed(new RuntimeException());
                break;
            case PROCESSING:
                ts.markRunning(engineID);
                break;
            case DONE:
                ts.markCompleted();
                break;
            default:
        }
        return ts;
    }

    @Override
    public boolean containsTask(TaskId id) {
        return redis.getState(id.getValue()).isPresent();
    }

    @Override
    public Set<TaskState> getTasks(@Nullable TaskStatus taskStatus, @Nullable String taskClassName,
            @Nullable String createdBy, @Nullable EngineID runningOnEngine, int limit, int offset) {
//        try (Jedis jedis = redis.getResource()) {
//            Stream<TaskState> stream = jedis.keys(PREFIX + "*").stream().map(key -> {
//                String v = jedis.get(key);
//                try {
//                    return (TaskState) deserialize(Base64.getDecoder().decode(v));
//                } catch (IllegalArgumentException e) {
//                    LOG.error("Could not decode key:value {}:{}", key, v);
//                    throw e;
//                }
//            });
//            if (taskStatus != null) {
//                stream = stream.filter(t -> t.status().equals(taskStatus));
//            }
//            if (taskClassName != null) {
//                stream = stream.filter(t -> t.taskClass().getName().equals(taskClassName));
//            }
//            if (createdBy != null) {
//                stream = stream.filter(t -> t.creator().equals(createdBy));
//            }
//            if (runningOnEngine != null) {
//                stream = stream
//                        .filter(t -> t.engineID() != null && t.engineID().equals(runningOnEngine));
//            }
//            stream = stream.skip(offset);
//            if (limit > 0) {
//                stream = stream.limit(limit);
//            }
//            Set<TaskState> results = stream.collect(toSet());
//            LOG.debug("getTasks returning {} results", results.size());
//            return results;
//        } catch (Exception e) {
//            throw GraknBackendException.stateStorageTaskRetrievalFailure(e);
//        }
        // TODO
        return Collections.emptySet();
    }

    @Override
    public void clear() {
        // TODO
    }
}
