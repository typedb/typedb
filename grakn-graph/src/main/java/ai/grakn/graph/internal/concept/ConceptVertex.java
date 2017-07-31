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

package ai.grakn.graph.internal.concept;

import ai.grakn.concept.Concept;
import ai.grakn.graph.internal.structure.VertexElement;

/**
 * <p>
 *     A {@link Concept} represented as a {@link VertexElement}
 * </p>
 *
 * <p>
 *     This class is helper used to ensure that any concept which needs to contain a {@link VertexElement} can handle it.
 *     Either by returning an existing one r going through some reification procedure to return a new one.
 * </p>
 *
 * @author fppt
 *
 */
public interface ConceptVertex {

    VertexElement vertex();

    static ConceptVertex from(Concept concept){
        return (ConceptVertex) concept;
    }
}
