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

package ai.grakn.engine.controller.response;

import ai.grakn.API;
import ai.grakn.engine.Jacksonisable;
import ai.grakn.util.REST;
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
@AutoValue
@JsonIgnoreProperties(value={"@id", "keyspace"}, allowGetters=true)
public abstract class Keyspaces implements Jacksonisable{

    @API
    @CheckReturnValue
    @JsonProperty
    public abstract Set<Keyspace> keyspaces();

    @API
    @CheckReturnValue
    @JsonCreator
    public static Keyspaces of(@JsonProperty("keyspaces") Set<Keyspace> keyspaces){
        return new AutoValue_Keyspaces(keyspaces);
    }

    @API
    @CheckReturnValue
    @JsonProperty("keyspace")
    public final Link keyspace() {
        return Link.create(REST.reformatTemplate(WebPath.KB_KEYSPACE));
    }

    @API
    @CheckReturnValue
    @JsonProperty("@id")
    public String id(){
        return WebPath.KB;
    }
}
