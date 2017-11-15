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

package ai.grakn.engine.controller.response;

import ai.grakn.util.REST.WebPath;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

import javax.annotation.CheckReturnValue;
import java.util.Set;

/**
 * <p>
 *     Response object representing a collection of {@link Keyspace}s
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@AutoValue
public abstract class Keyspaces {

    @CheckReturnValue
    @JsonProperty
    public abstract Set<Keyspace> keyspaces();

    @CheckReturnValue
    @JsonCreator
    public static Keyspaces of(@JsonProperty("keyspaces") Set<Keyspace> keyspaces){
        return new AutoValue_Keyspaces(keyspaces);
    }

    @CheckReturnValue
    @JsonProperty("@id")
    public String id(){
        return WebPath.KB;
    }
}
