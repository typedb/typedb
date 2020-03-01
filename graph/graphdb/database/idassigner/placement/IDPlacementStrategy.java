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

package grakn.core.graph.graphdb.database.idassigner.placement;

import grakn.core.graph.graphdb.internal.InternalElement;
import grakn.core.graph.graphdb.internal.InternalVertex;

import java.util.List;
import java.util.Map;

/**
 * Determines how vertices are placed in individual graph partitions.
 * A particular implementation determines the partition id of a (newly created) vertex. The vertex is
 * then assigned to said partition by JanusGraph.
 * <p>
 * Janus also had PropertyPlacementStrategy (now removed from Grakn) - might want to look into that for some kind of inspo maybe.
 */
public interface IDPlacementStrategy {

    /**
     * Individually assigns an id to the given vertex or relation.
     *
     * @param element Vertex or relation to assign id to.
     */
    int getPartition(InternalElement element);

    /**
     * Bulk assignment of idAuthorities to vertices.
     * <p>
     * It is expected that the passed in map contains the partition assignment after this method
     * returns. Any initial values in the map are meaningless and to be ignored.
     * <p>
     * This is an optional operation. Check with #supportsBulkPlacement() first.
     *
     * @param vertices Map containing all vertices and their partition placement.
     */
    void getPartitions(Map<InternalVertex, PartitionAssignment> vertices);

    /**
     * Whether this placement strategy supports bulk placement.
     * If not, then #getPartitions(Map) will throw UnsupportedOperationException
     */
    boolean supportsBulkPlacement();

    /**
     * If JanusGraph is embedded, this method is used to indicate to the placement strategy which
     * part of the partition id space is hosted locally so that vertex and edge placements can be made accordingly
     * (i.e. preferring to assign partitions in the local ranges so that the data is hosted locally which is often
     * faster).
     * <p>
     * This method can be called at any time while JanusGraph is running. It is typically called right
     * after construction and when the id space is redistributed.
     * <p>
     * Depending on the storage backend one or multiple ranges of partition ids may be given. However, this list is never
     * empty.
     *
     * @param localPartitionIdRanges List of PartitionIDRanges corresponding to the locally hosted partitions
     */
    void setLocalPartitionBounds(List<PartitionIDRange> localPartitionIdRanges);

    /**
     * Called when there are no more idAuthorities left in the given partition. It is expected that the
     * placement strategy will no longer use said partition in its placement suggestions.
     *
     * @param partitionID Id of the partition that has been exhausted.
     */
    void exhaustedPartition(int partitionID);

}
