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

package ai.grakn.engine.controller.response;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.util.Schema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

import java.util.Set;

/**
 * <p>
 *     Wrapper class for {@link ai.grakn.concept.RelationshipType}
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
@AutoValue
public abstract class RelationshipType extends Type{

    @JsonProperty
    public abstract Set<Link> relates();

    @JsonCreator
    public static RelationshipType create(
            @JsonProperty("id") ConceptId id,
            @JsonProperty("@id") Link selfLink,
            @JsonProperty("label") Label label,
            @JsonProperty("implicit") Boolean implicit,
            @JsonProperty("super") EmbeddedSchemaConcept sup,
            @JsonProperty("subs") Link subs,
            @JsonProperty("abstract") Boolean isAbstract,
            @JsonProperty("plays") Link plays,
            @JsonProperty("attributes") Link attributes,
            @JsonProperty("keys") Link keys,
            @JsonProperty("instances") Link instances,
            @JsonProperty("relates") Set<Link> relates){
        return new AutoValue_RelationshipType(Schema.BaseType.RELATIONSHIP_TYPE.name(), id, selfLink, label, implicit, sup, subs, isAbstract, plays, attributes, keys, instances, relates);
    }
}
