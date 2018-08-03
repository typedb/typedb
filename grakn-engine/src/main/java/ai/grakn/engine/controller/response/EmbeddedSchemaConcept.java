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

import ai.grakn.concept.Label;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

/**
 * <p>
 *     Wrapper class for a light representation of {@link ai.grakn.concept.Type} which is embedded in the
 *     {@link Thing} representation
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
@AutoValue
public abstract class EmbeddedSchemaConcept {

    @JsonProperty("@id")
    public abstract Link selfLink();

    @JsonProperty
    public abstract Label label();

    @JsonCreator
    public static EmbeddedSchemaConcept create(@JsonProperty("@id") Link selfLink, @JsonProperty("label") Label label){
        return new AutoValue_EmbeddedSchemaConcept(selfLink, label);
    }

    public static EmbeddedSchemaConcept create(ai.grakn.concept.SchemaConcept schemaConcept){
        return create(Link.create(schemaConcept), schemaConcept.label());
    }
}
