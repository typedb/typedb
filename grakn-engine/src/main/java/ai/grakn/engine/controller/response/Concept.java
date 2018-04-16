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
import ai.grakn.engine.Jacksonisable;
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
