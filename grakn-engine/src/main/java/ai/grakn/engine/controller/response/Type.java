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

import ai.grakn.util.Schema;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * <p>
 *     Wrapper class for {@link ai.grakn.concept.Type}
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public abstract class Type extends SchemaConcept{

    @Nullable
    @JsonProperty("abstract")
    public abstract Boolean isAbstract();

    @Nullable
    public abstract Set<Role> rolesPlayed();

    @Nullable
    public abstract Set<AttributeType> attributesLinked();

    @Nullable
    public abstract Set<AttributeType> keysLinked();

    @JsonProperty
    public Set<String> plays(){
        return extractIds(rolesPlayed());
    }

    @JsonProperty
    public Set<String> attributes(){
        return extractIds(attributesLinked());
    }

    @JsonProperty
    public Set<String> keys(){
        return extractIds(keysLinked());
    }

    @Override
    public Schema.BaseType baseType(){
        return Schema.BaseType.TYPE;
    }
}
