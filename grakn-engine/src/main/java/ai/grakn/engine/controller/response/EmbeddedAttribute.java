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

import ai.grakn.engine.Jacksonisable;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

/**
 * <p>
 *     Wrapper class for a light representation of {@link ai.grakn.concept.Attribute}s which are embedded in the
 *     {@link Thing} representations
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
@AutoValue
public abstract class EmbeddedAttribute implements Jacksonisable {

    @JsonProperty("@id")
    public abstract Link selfLink();

    @JsonProperty
    public abstract EmbeddedType type();

    @JsonProperty
    public abstract String value();

    @JsonProperty("data-type")
    public abstract String dataType();


    @JsonCreator
    public static EmbeddedAttribute create(
            @JsonProperty("@id") Link selfLink,
            @JsonProperty("type") EmbeddedType type,
            @JsonProperty("value") String value,
            @JsonProperty("data-type") String dataType
    ){
        return new AutoValue_EmbeddedAttribute(selfLink, type, value, dataType);
    }

    public static EmbeddedAttribute create(ai.grakn.concept.Attribute attribute){
        return create(Link.create(attribute), EmbeddedType.create(attribute.type()), attribute.getValue().toString(), attribute.dataType().getName());
    }
}
