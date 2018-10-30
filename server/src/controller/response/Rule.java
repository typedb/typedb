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

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.util.Schema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;

/**
 * <p>
 *     Wrapper class for {@link ai.grakn.concept.Rule}
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
@AutoValue
public abstract class Rule extends SchemaConcept{

    @Nullable
    @JsonProperty
    public abstract String when();

    @Nullable
    @JsonProperty
    public abstract String then();

    @JsonCreator
    public static Rule create(
            @JsonProperty("id") ConceptId id,
            @JsonProperty("@id") Link selfLink,
            @JsonProperty("label") Label label,
            @JsonProperty("implicit") Boolean implicit,
            @JsonProperty("super") EmbeddedSchemaConcept sup,
            @JsonProperty("subs") Link subs,
            @Nullable @JsonProperty("when") String when,
            @Nullable @JsonProperty("then") String then){
        return new AutoValue_Rule(Schema.BaseType.RULE.name(), id, selfLink, label, implicit, sup, subs, when, then);
    }
}
