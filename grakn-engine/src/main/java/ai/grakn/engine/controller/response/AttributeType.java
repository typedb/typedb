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
import ai.grakn.util.Schema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * <p>
 *     Wrapper class for {@link ai.grakn.concept.AttributeType}
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
@AutoValue
public abstract class AttributeType extends Type{

    @Nullable
    @JsonProperty("data-type")
    public abstract String dataType();

    @Nullable
    @JsonProperty
    public abstract String regex();

    @JsonCreator
    public static AttributeType create(
            @JsonProperty("id") ConceptId id,
            @JsonProperty("@id") Link selfLink,
            @JsonProperty("label") Label label,
            @JsonProperty("implicit") Boolean implicit,
            @JsonProperty("super") Link sup,
            @JsonProperty("subs") Set<Link> subs,
            @JsonProperty("abstract") Boolean isAbstract,
            @JsonProperty("plays") Set<Link> plays,
            @JsonProperty("attributes") Set<Link> attributes,
            @JsonProperty("keys") Set<Link> keys,
            @Nullable @JsonProperty("data-type") String dataType,
            @Nullable @JsonProperty("regex") String regex){
        return new AutoValue_AttributeType(Schema.BaseType.ATTRIBUTE_TYPE.name(),id, selfLink, label, implicit, sup, subs, isAbstract, plays, attributes, keys, dataType, regex);
    }
}
