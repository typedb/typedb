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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.controller.response;

import ai.grakn.concept.ConceptId;
import ai.grakn.util.Schema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;

/**
 * <p>
 *     Wrapper class for {@link ai.grakn.concept.Attribute}
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
@AutoValue
public abstract class Attribute extends Thing {

    @JsonProperty("data-type")
    public abstract String dataType();

    @JsonProperty
    public abstract String value();

    @JsonCreator
    public static Attribute create(
            @JsonProperty("id") ConceptId id,
            @JsonProperty("@id") Link selfLink,
            @JsonProperty("type") EmbeddedSchemaConcept type,
            @JsonProperty("attributes") Link attributes,
            @JsonProperty("keys") Link keys,
            @JsonProperty("relationships") Link relationships,
            @JsonProperty("inferred") boolean inferred,
            @Nullable @JsonProperty("explanation-query")  String explanation,
            @JsonProperty("data-type") String dataType,
            @JsonProperty("value") String value){
        return new AutoValue_Attribute(Schema.BaseType.ATTRIBUTE.name(), id, selfLink, type, attributes, keys, relationships, inferred, explanation, dataType, value);
    }
}
