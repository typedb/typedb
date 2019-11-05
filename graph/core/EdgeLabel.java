/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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


package grakn.core.graph.core;

import java.util.Collection;

/**
 * EdgeLabel is an extension of {@link RelationType} for edges. Each edge in JanusGraph has a label.
 * <p>
 * An edge label defines the following characteristics of an edge:
 * <ul>
 * <li><strong>Directionality:</strong> An edge is either directed or unidirected. A directed edge can be thought of
 * as a "normal" edge: it points from one vertex to another and both vertices are aware of the edge's existence. Hence
 * the edge can be traversed in both directions. A unidirected edge is like a hyperlink in that only the out-going
 * vertex is aware of its existence and it can only be traversed in the outgoing direction.</li>
 * <li><strong>Multiplicity:</strong> The multiplicity of an edge imposes restrictions on the number of edges
 * for a particular label that are allowed on a vertex. This allows the definition and enforcement of domain constraints.
 * </li>
 * </ul>
 *
 * @see RelationType
 */
public interface EdgeLabel extends RelationType {

    /**
     * Checks whether this labels is defined as directed.
     *
     * @return true, if this label is directed, else false.
     */
    boolean isDirected();

    /**
     * Checks whether this labels is defined as unidirected.
     *
     * @return true, if this label is unidirected, else false.
     */
    boolean isUnidirected();

    /**
     * The {@link Multiplicity} for this edge label.
     */
    Multiplicity multiplicity();

    /**
     * Collects all property constraints.
     *
     * @return a list of {@link PropertyKey} which represents all property constraints for a {@link EdgeLabel}.
     */
    Collection<PropertyKey> mappedProperties();

    /**
     * Collects all connection constraints.
     *
     * @return a list of {@link Connection} which represents all connection constraints for a {@link EdgeLabel}.
     */
    Collection<Connection> mappedConnections();

}
