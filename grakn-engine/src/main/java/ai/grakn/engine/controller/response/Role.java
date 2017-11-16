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

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

import java.util.Set;

/**
 * <p>
 *     Wrapper class for {@link ai.grakn.concept.Role}
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
@AutoValue
public abstract class Role extends SchemaConcept{

    @JsonProperty
    public abstract Set<Link> relationships();

    @JsonProperty
    public abstract Set<Link> roleplayers();

    @JsonCreator
    public static Role create(
            ConceptId id,
            @JsonProperty("@id") Link selfLink,
            Label label,
            Boolean implicit,
            @JsonProperty("super") Link sup,
            Set<Link> subs,
            Set<Link> relationships,
            Set<Link> roleplayers){
        return new AutoValue_Role(id, selfLink, label, implicit, sup, subs, relationships, roleplayers);
    }
}
