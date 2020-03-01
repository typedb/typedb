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

package grakn.core.graph.core.schema;

import grakn.core.graph.core.VertexLabel;

/**
 * A builder to create new VertexLabels.
 *
 * A vertex label is defined by its name and additional properties such as:
 * <ul>
 *     <li>Partition: Whether the vertices of this label should be partitioned. A partitioned vertex is split across the partitions
 *     in a graph such that each partition contains on "sub-vertex". This allows JanusGraph to effectively manage
 *     vertices with very large degrees but is inefficient for vertices with small degree</li>
 * </ul>
 *
 */
public interface VertexLabelMaker {

    /**
     * Returns the name of the to-be-build vertex label
     * @return the label name
     */
    String getName();

    /**
     * Enables partitioning for this vertex label. If a vertex label is partitioned, all of its
     * vertices are partitioned across the partitions of the graph.
     *
     * @return this VertexLabelMaker
     */
    VertexLabelMaker partition();

    /**
     * Makes this vertex label static, which means that vertices of this label cannot be modified outside of the transaction
     * in which they were created.
     *
     * @return this VertexLabelMaker
     */
    VertexLabelMaker setStatic();

    /**
     * Creates a VertexLabel according to the specifications of this builder.
     *
     * @return the created vertex label
     */
    VertexLabel make();


}
