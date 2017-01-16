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

package ai.grakn.graql.internal.analytics;

import ai.grakn.concept.TypeName;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class Utility {
    /**
     * The Grakn type property on a given Tinkerpop vertex.
     *
     * @param vertex the Tinkerpop vertex
     * @return the type
     */
    static TypeName getVertexType(Vertex vertex) {
        return TypeName.of(vertex.value(Schema.ConceptProperty.TYPE.name()));
    }

    /**
     * Whether the Tinkerpop vertex has a Grakn type property reserved for analytics.
     *
     * @param vertex the Tinkerpop vertex
     * @return if the type is reserved or not
     */
    static boolean isAnalyticsElement(Vertex vertex) {
        return CommonOLAP.analyticsElements.contains(getVertexType(vertex));
    }

    /**
     * The state of the vertex in the database. This may detect ghost nodes and allow them to be excluded from
     * computations. If the vertex is alive it is likely to be a valid Grakn concept.
     *
     * @return if the vertex is alive
     */
    static boolean isAlive(Vertex vertex) {
        if (vertex == null) return false;

        try {
            return vertex.property(Schema.BaseType.TYPE.name()).isPresent();
        } catch (IllegalStateException e) {
            return false;
        }
    }
}
