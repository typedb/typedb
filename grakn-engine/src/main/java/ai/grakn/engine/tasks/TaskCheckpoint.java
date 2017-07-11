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

package ai.grakn.engine.tasks;

import java.io.Serializable;
import java.time.Instant;
import mjson.Json;

import static java.time.Instant.now;

/**
 * Internal checkpoint used to keep track of task execution
 *
 * @author alexandraorth
 */
public class TaskCheckpoint implements Serializable {

    private static final long serialVersionUID = -7301340972479426643L;

    private final Json checkpoint;
    private final Instant createdAt;

    public static TaskCheckpoint of(Json checkpoint){
        return new TaskCheckpoint(checkpoint);
    }

    private TaskCheckpoint(Json checkpoint){
        this.checkpoint = checkpoint;
        this.createdAt = now();
    }

    public Json checkpoint(){
        return checkpoint;
    }

    public Instant createdAt(){
        return createdAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TaskCheckpoint that = (TaskCheckpoint) o;

        return checkpoint.toString().equals(that.checkpoint.toString()) && createdAt.equals(that.createdAt);
    }

    @Override
    public int hashCode() {
        int result = checkpoint != null ? checkpoint.hashCode() : 0;
        result = 31 * result + (createdAt != null ? createdAt.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TaskCheckpoint.of(" + checkpoint + ")";
    }
}
