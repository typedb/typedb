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
 */

package grakn.core.graph.core;

import grakn.core.graph.core.schema.JanusGraphSchemaType;

/**
 * A vertex label is a label attached to vertices in a JanusGraph graph. This can be used to define the nature of a
 * vertex.
 * <p>
 * Internally, a vertex label is also used to specify certain characteristics of vertices that have a given label.
 */
public interface VertexLabel extends JanusGraphVertex, JanusGraphSchemaType {

    /**
     * Whether vertices with this label are partitioned.
     */
    boolean isPartitioned();

    /**
     * Whether vertices with this label are static, that is, immutable beyond the transaction
     * in which they were created.
     */
    boolean isStatic();

}
