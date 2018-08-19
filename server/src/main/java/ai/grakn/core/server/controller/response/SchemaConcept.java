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

import ai.grakn.concept.Label;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/**
 * <p>
 *     Wrapper class for {@link ai.grakn.concept.SchemaConcept}
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public abstract class SchemaConcept extends Concept{

    @JsonProperty
    public abstract Label label();

    @JsonProperty
    public abstract Boolean implicit();

    @Nullable
    @JsonProperty("super")
    public abstract EmbeddedSchemaConcept sup();

    @JsonProperty
    public abstract Link subs();

}

