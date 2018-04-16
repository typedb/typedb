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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.auto.value.AutoValue;

import javax.annotation.CheckReturnValue;
import java.util.UUID;

/**
 * An identifier for a task
 *
 * @author Felix Chapman
 */
@AutoValue
public abstract class TaskId {

    @CheckReturnValue
    @JsonCreator
    public static TaskId of(String value) {
        return new AutoValue_TaskId(value);
    }

    @CheckReturnValue
    public static TaskId generate() {
        return new AutoValue_TaskId(UUID.randomUUID().toString());
    }

    /**
     * Get the string value of the task ID
     */
    @CheckReturnValue
    @JsonValue
    public abstract String value();
}
