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

import ai.grakn.engine.tasks.TaskConfiguration;
import ai.grakn.engine.tasks.TaskState;
import com.google.auto.value.AutoValue;
import java.io.Serializable;

/**
 * Convenience class that includes a task state and config
 *
 * @author Domenico Corapi
 */
@AutoValue
abstract class Task implements QueableTask, Serializable {
    public abstract String getId();
    public abstract TaskState getTaskState();
    public abstract TaskConfiguration getTaskConfiguration();

    public static Builder builder() {
        return new AutoValue_Task.Builder();
    }



    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder setId(String newId);
        public abstract Builder setTaskState(TaskState newTaskState);
        public abstract Builder setTaskConfiguration(TaskConfiguration newTaskConfiguration);
        public abstract Task build();
    }
}
