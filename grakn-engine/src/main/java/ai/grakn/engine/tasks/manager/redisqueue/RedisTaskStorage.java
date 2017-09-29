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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.codahale.metrics.MetricRegistry.name;


/**
 * DAO for redis task states
 *
 * @author Domenico Corapi
 */
public class RedisTaskStorage implements TaskStateStorage {

    private static final Logger LOG = LoggerFactory.getLogger(RedisTaskStorage.class);
    private final Meter writeError;

    private Redisq<Task> redis;

    private RedisTaskStorage(Redisq<Task> redis, MetricRegistry metricRegistry) {
        this.redis = redis;
        this.writeError = metricRegistry.meter(name(RedisTaskStorage.class, "write", "error"));
    }

    public static RedisTaskStorage create(Redisq<Task> redisq, MetricRegistry metricRegistry) {
        return new RedisTaskStorage(redisq, metricRegistry);
    }

    private State mapStatus(TaskStatus status) {
        return status.asStateInfo();
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

        TaskState taskState = TaskState.of(id, TaskStatus.fromState(state.get().getState()));

        //Make sure exception gets transferred across
        if(taskState.status().equals(TaskStatus.FAILED)) {
            String info = state.get().getInfo();
            taskState.markFailed(info);
        }

        return taskState;
    }

    @Override
    public boolean containsTask(TaskId id) {
        return redis.getState(id.getValue()).isPresent();
    }

    @Override
    public Set<TaskState> getTasks(@Nullable TaskStatus taskStatus, @Nullable String taskClassName,
            @Nullable String createdBy, @Nullable EngineID runningOnEngine, int limit, int offset) {
        Stream<TaskState> stream = redis.getStates().filter(Optional::isPresent).map(Optional::get)
                .map(s -> TaskState.of(TaskId.of(s.getId()), TaskStatus.fromState(s.getStateInfo().getState())));

        if (taskStatus != null) {
            stream = stream.filter(t -> t.status().equals(taskStatus));
        }
        if (taskClassName != null) {
            LOG.warn("Asked for taskClassName filter but not implemented");
        }
        if (createdBy != null) {
            LOG.warn("Asked for createdBy filter but not implemented");
        }
        if (runningOnEngine != null) {
            LOG.warn("Asked for runningOnEngine filter but not implemented");
        }
        stream = stream.skip(offset);
        if (limit > 0) {
            stream = stream.limit(limit);
        }
        Set<TaskState> results = stream.collect(Collectors.toSet());
        LOG.debug("getTasks returning {} results", results.size());
        return results;
    }

    @Override
    public void clear() {
        // TODO
    }
}
