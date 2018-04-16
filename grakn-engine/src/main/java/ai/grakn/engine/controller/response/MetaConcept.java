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

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.util.Schema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;

/**
 * <p>
 *     Special wrapper class for the top meta concept {@link ai.grakn.util.Schema.MetaSchema#THING};
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
@JsonIgnoreProperties(value={ "abstract", "plays", "attributes", "keys", "implicit" }, allowGetters=true)
@AutoValue
public abstract class MetaConcept extends Type{

    @JsonCreator
    public static MetaConcept create(
            @JsonProperty("id") ConceptId id,
            @JsonProperty("@id") Link selfLink,
            @JsonProperty("label") Label label,
            @JsonProperty("super") @Nullable EmbeddedSchemaConcept sup,
            @JsonProperty("subs") Link subs,
            @JsonProperty("plays") Link plays,
            @JsonProperty("attributes") Link attributes,
            @JsonProperty("keys") Link keys,
            @JsonProperty("instances") Link instances
    ){
        return new AutoValue_MetaConcept(Schema.BaseType.TYPE.name(), id, selfLink, label, false, sup, subs, true, plays, attributes, keys, instances);
    }
}
