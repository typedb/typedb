/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.concept.impl;

import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.structure.VertexElement;

/**
 * A Concept represented as a VertexElement
 * This class is helper used to ensure that any concept which needs to contain a VertexElement can handle it.
 * Either by returning an existing one r going through some reification procedure to return a new one.
 */
public interface ConceptVertex {

    static ConceptVertex from(Concept concept) {
        return (ConceptVertex) concept;
    }

    /**
     * @return corresponding Janus vertex
     */
    VertexElement vertex();

    /**
     * @return the id of the Janus vertex
     */
    default Object elementId(){ return vertex().id();}
}
