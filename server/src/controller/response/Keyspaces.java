/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.core.server.controller.response;

import ai.grakn.core.server.Jacksonisable;
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

    @CheckReturnValue
    @JsonProperty
    public abstract Set<Keyspace> keyspaces();

    @CheckReturnValue
    @JsonCreator
    public static Keyspaces of(@JsonProperty("keyspaces") Set<Keyspace> keyspaces){
        return new AutoValue_Keyspaces(keyspaces);
    }

    @CheckReturnValue
    @JsonProperty("keyspace")
    public final Link keyspace() {
        return Link.create(REST.reformatTemplate(WebPath.KB_KEYSPACE));
    }

    @CheckReturnValue
    @JsonProperty("@id")
    public String id(){
        return WebPath.KB;
    }
}
