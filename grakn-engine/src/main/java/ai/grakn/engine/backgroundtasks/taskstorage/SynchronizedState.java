/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016 Grakn Labs Ltd
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

package ai.grakn.engine.backgroundtasks.taskstorage;

import ai.grakn.engine.backgroundtasks.TaskStatus;
import org.json.JSONObject;

import static ai.grakn.engine.util.SystemOntologyElements.ENGINE_ID;
import static ai.grakn.engine.util.SystemOntologyElements.STATUS;
import static ai.grakn.engine.util.SystemOntologyElements.TASK_CHECKPOINT;

/**
 * State to be stored in Zookeeper
 */
public class SynchronizedState {
    private TaskStatus status;
    private String engineID;
    private String checkpoint;

    public SynchronizedState(TaskStatus status) {
        this.status = status;
    }

    public SynchronizedState status(TaskStatus status) {
        this.status = status;
        return this;
    }

    public TaskStatus status() {
        return status;
    }

    public SynchronizedState engineID(String engineID) {
        this.engineID = engineID;
        return this;
    }

    public String engineID() {
        return engineID;
    }

    public SynchronizedState checkpoint(String checkpoint) {
        this.checkpoint = checkpoint;
        return this;
    }

    public String checkpoint() {
        return checkpoint;
    }

    String serialize() {
        JSONObject json = new JSONObject();
        json.put(STATUS.getValue(), status)
            .put(ENGINE_ID.getValue(), engineID)
            .put(TASK_CHECKPOINT.getValue(), checkpoint);

        return json.toString();
    }

    public static SynchronizedState deserialize(String serialized){
        JSONObject json = new JSONObject(serialized);
        TaskStatus status = TaskStatus.valueOf(json.getString(STATUS.getValue()));

        SynchronizedState state = new SynchronizedState(status);
        state = json.has(ENGINE_ID.getValue()) ? state.engineID(json.getString(ENGINE_ID.getValue())) : state;
        state = json.has(TASK_CHECKPOINT.getValue()) ? state.checkpoint(json.getString(TASK_CHECKPOINT.getValue())) : state;
        return state;
    }
}
