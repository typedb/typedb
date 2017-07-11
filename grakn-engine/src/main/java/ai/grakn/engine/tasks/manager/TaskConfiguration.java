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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import mjson.Json;

/**
 * Internal checkpoint used to keep track of task execution
 *
 * @author alexandraorth
 */

public class TaskConfiguration implements Serializable {

    private static final long serialVersionUID = -7301340972479426643L;

    private final Json configuration;

    public static TaskConfiguration of(Json configuration){
        return new TaskConfiguration(configuration);
    }

    public TaskConfiguration(Json configuration){
        this.configuration = configuration;
    }

    @JsonCreator
    public TaskConfiguration(@JsonProperty("configuration") String configuration){
        this.configuration = Json.read(configuration);
    }

    public Json json(){
        return configuration;
    }

    public Json configuration(){
        return configuration;
    }

    @JsonProperty
    public String getConfiguration(){
        return configuration.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TaskConfiguration that = (TaskConfiguration) o;

        return configuration.toString().equals(that.configuration.toString());
    }

    @Override
    public int hashCode() {
        int result = configuration != null ? configuration.hashCode() : 0;
        return result;
    }

    @Override
    public String toString() {
        return "TaskConfiguration.of(" + configuration + ")";
    }
}
