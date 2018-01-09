/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
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

package ai.grakn.engine.tasks.manager;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

import java.io.Serializable;

/**
 * Stores the configuration necessary to run {@link ai.grakn.engine.tasks.manager.redisqueue.Task}s.
 * This is used by the {@link TaskManager}
 *
 * @author alexandraorth, Filipe Peliz Pinto Teixeira
 */
@AutoValue
public abstract class TaskConfiguration implements Serializable {
    private static final long serialVersionUID = -7301340972479426643L;

    @JsonProperty
    public abstract String configuration();

    @JsonCreator
    public static TaskConfiguration of(@JsonProperty("configuration") String configuration){
        return new AutoValue_TaskConfiguration(configuration);
    }

}
