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
package ai.grakn.client;

import ai.grakn.engine.TaskId;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.auto.value.AutoValue;

import static ai.grakn.util.REST.Response.Task.STACK_TRACE;

/**
 * Wrapper for handling the outcome of the execution of a task
 *
 * @author Domenico Corapi
 */
@AutoValue
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = AutoValue_TaskResult.Builder.class)
public abstract class TaskResult {
    @JsonProperty("taskId")
    public abstract TaskId getTaskId();
    @JsonProperty(STACK_TRACE)
    public abstract String getStackTrace();
    @JsonProperty("code")
    public abstract String getCode();

    public static Builder builder() {
        return new AutoValue_TaskResult.Builder();
    }

    /**
     * Builder
     *
     * @author Domenico Corapi
     */
    @AutoValue.Builder
    public abstract static class Builder {
        @JsonProperty("taskId")
        public abstract Builder setTaskId(TaskId taskId);
        @JsonProperty(STACK_TRACE)
        public abstract Builder setStackTrace(String stackTrace);
        @JsonProperty("code")
        public abstract Builder setCode(String code);

        public abstract TaskResult build();
    }
}
