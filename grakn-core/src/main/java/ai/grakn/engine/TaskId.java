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

package ai.grakn.engine;

import java.util.UUID;

/**
 * An identifier for a task
 *
 * @author Felix Chapman
 */
public final class TaskId {
    private final String value;

    public static TaskId of(String value) {
        return new TaskId(value);
    }

    public static TaskId generate() {
        return new TaskId(UUID.randomUUID().toString());
    }

    private TaskId(String value) {
        this.value = value;
    }

    /**
     * Get the string value of the task ID
     */
    public String getValue() {
        return value;
    }

    public String toString() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TaskId varName = (TaskId) o;

        return value.equals(varName.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}
