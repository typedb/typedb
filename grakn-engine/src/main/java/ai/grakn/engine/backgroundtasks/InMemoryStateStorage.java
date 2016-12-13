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

package ai.grakn.engine.backgroundtasks;

import javafx.util.Pair;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryStateStorage implements StateStorage {
    private static InMemoryStateStorage instance = null;

    private Map<String, TaskState> storage;

    private InMemoryStateStorage() {
        storage = new ConcurrentHashMap<>();
    }

    public static synchronized InMemoryStateStorage getInstance() {
        if(instance == null)
            instance = new InMemoryStateStorage();
        return instance;
    }

    public String newState(String taskName, String createdBy, Date runAt, Boolean recurring, long interval, JSONObject configuration) {
        if(taskName == null || createdBy == null || runAt == null || recurring == null)
            return null;

        TaskState state = new TaskState(taskName);
        state.creator(createdBy)
             .runAt(runAt)
             .isRecurring(recurring)
             .interval(interval);

        if(configuration != null)
             state.configuration(configuration);
        else
            state.configuration(new JSONObject());

        String id = UUID.randomUUID().toString();
        storage.put(id, state);

        return id;
    }

    public void updateState(String id, TaskStatus status, String statusChangeBy, String executingHostname,
                            Throwable failure, String checkpoint, JSONObject configuration) {
        if(id == null)
            return;

        if(status == null && statusChangeBy == null && executingHostname == null && failure == null
                && checkpoint == null && configuration == null)
            return;

        TaskState state = storage.get(id);
        synchronized (state) {
            state.status(status);

            if(statusChangeBy != null)
                state.statusChangedBy(statusChangeBy);
            if(executingHostname != null)
                state.executingHostname(executingHostname);
            if(failure != null)
                state.exception(failure.toString())
                     .stackTrace(Arrays.toString(failure.getStackTrace()));
            if(checkpoint != null)
                state.checkpoint(checkpoint);
            if(configuration != null)
                state.configuration(configuration);
        }

        if (status == TaskStatus.COMPLETED || status == TaskStatus.FAILED) {
            state.configuration(new JSONObject());
        }
    }

    public TaskState getState(String id) {
        if(id == null || !storage.containsKey(id))
            return null;

        TaskState state = storage.get(id);
        TaskState newState = null;

        synchronized (state) {
            try {
                newState = state.clone();
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
        }

        return newState;
    }

    public Set<Pair<String, TaskState>> getTasks(TaskStatus taskStatus, String taskClassName, String createdBy, int limit, int offset) {
        Set<Pair<String, TaskState>> res = new HashSet<>();

        int count = 0;
        for(Map.Entry<String, TaskState> x: storage.entrySet()) {
            TaskState state = x.getValue();

            // AND
            if(taskStatus != null && state.status() != taskStatus)
                continue;
            if(taskClassName != null && !Objects.equals(state.taskClassName(), taskClassName))
                continue;
            if(createdBy != null && !Objects.equals(state.creator(), createdBy))
                continue;

            if(count < offset) {
                count++;
                continue;
            }
            else if(limit > 0 && count >= (limit+offset)) {
                break;
            }
            count++;

            res.add(new Pair<>(x.getKey(), state));
        }

        return res;
    }
}
