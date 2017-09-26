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

package ai.grakn.engine.tasks.manager;

import ai.grakn.engine.TaskId;
import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.util.EngineID;
import ai.grakn.exception.GraknBackendException;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 *     Implementation of StateStorage that stores task state in memory.
 *     If engine fails, task state recovery is not possible.
 * </p>
 *
 * @author Denis Lobanov, alexandraorth
 */
public class TaskStateInMemoryStore implements TaskStateStorage {

    private final Map<TaskId, TaskState> storage = new ConcurrentHashMap<>();
    private final Queue<TaskId> finishedTasks = new ArrayBlockingQueue<>(MAX_FINISHED_TASKS);

    private static final int MAX_FINISHED_TASKS = 10_000;

    public TaskStateInMemoryStore() {
    }

    @Override
    public TaskId newState(TaskState state) {
        updateState(state);
        return state.getId();
    }

    @Override
    public Boolean updateState(TaskState state) {
        TaskId taskId = state.getId();

        boolean taskFinished = state.getStatus() == TaskStatus.COMPLETED || state.getStatus() == TaskStatus.FAILED;

        if (taskFinished) {
            while (!finishedTasks.offer(taskId)) {
                TaskId oldestFinishedTask = finishedTasks.poll();
                storage.remove(oldestFinishedTask);
            }
        }

        storage.put(taskId, state.copy());
        return true;
    }

    @Override
    public TaskState getState(TaskId id) {
        Optional<TaskState> taskState = Optional.ofNullable(storage.get(id));

        if(!taskState.isPresent()) {
            throw GraknBackendException.stateStorageMissingId(id);
        }

        return taskState.get().copy();
    }

    @Override
    public boolean containsTask(TaskId id) {
        return storage.containsKey(id);
    }

    @Override
    public Set<TaskState> getTasks(@Nullable TaskStatus taskStatus, @Nullable String taskClassName,
            @Nullable String createdBy, @Nullable EngineID engineRunningOn, int limit, int offset) {
        Set<TaskState> res = new HashSet<>();

        int count = 0;
        for(Map.Entry<TaskId, TaskState> x: storage.entrySet()) {
            TaskState state = x.getValue();
            if(state == null) {
                continue;
            }

            // AND
            if(taskStatus != null && state.status() != taskStatus) {
                continue;
            }
            if(taskClassName != null && !Objects.equals(state.taskClass().getName(), taskClassName)) {
                continue;
            }
            if(createdBy != null && !Objects.equals(state.creator(), createdBy)) {
                continue;
            }
            if(engineRunningOn != null && !Objects.equals(state.engineID(), engineRunningOn)){
                continue;
            }

            if(count < offset) {
                count++;
                continue;
            }
            else if(limit > 0 && count >= (limit+offset)) {
                break;
            }
            count++;

            res.add(state.copy());
        }

        return res;
    }

    @Override
    public void clear() {
        storage.clear();
        finishedTasks.clear();
    }
}
