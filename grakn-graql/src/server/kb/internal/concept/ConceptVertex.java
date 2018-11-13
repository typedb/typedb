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

package grakn.core.server.kb.internal.concept;

import grakn.core.concept.Concept;
import grakn.core.server.kb.internal.structure.VertexElement;

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
 *
 */
public interface ConceptVertex {

    VertexElement vertex();

    static ConceptVertex from(Concept concept){
        return (ConceptVertex) concept;
    }
}
