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
import ai.grakn.core.server.Jacksonisable;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * <p>
 *     Wrapper class for {@link ai.grakn.concept.Concept}
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public abstract class Concept implements Jacksonisable{

    @JsonProperty("base-type")
    public abstract String baseType();

    @JsonProperty("id")
    public abstract ConceptId id();

    @JsonProperty("@id")
    public abstract Link selfLink();

}
