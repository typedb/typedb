/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

import ai.grakn.engine.tasks.manager.TaskConfiguration;
import ai.grakn.engine.tasks.manager.TaskState;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.auto.value.AutoValue;

import java.io.Serializable;

/**
 * Convenience class that includes a task state and config
 *
 * @author Domenico Corapi
 */
@AutoValue
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = AutoValue_Task.Builder.class)
abstract public class Task implements Serializable{
    protected static final long serialVersionUID = 42L;

    @JsonProperty("taskState")
    public abstract TaskState getTaskState();

    @JsonProperty("taskConfiguration")
    public abstract TaskConfiguration getTaskConfiguration();

    public static Builder builder() {
        return new AutoValue_Task.Builder();
    }

    /**
     * Builder
     *
     * @author Domenico Corapi
     */
    @AutoValue.Builder
    public abstract static class Builder {
        @JsonProperty("taskState")
        public abstract Builder setTaskState(TaskState newTaskState);
        @JsonProperty("taskConfiguration")
        public abstract Builder setTaskConfiguration(TaskConfiguration newTaskConfiguration);
        public abstract Task build();
    }
}
