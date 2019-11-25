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
 *
 */

package grakn.core.kb.concept.structure;

import grakn.core.core.Schema;
import grakn.core.kb.concept.api.LabelId;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Type;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;
import java.util.stream.Stream;

public interface VertexElement extends AbstractElement<Vertex, Schema.VertexProperty> {
    EdgeElement addEdge(VertexElement to, Schema.EdgeLabel type);
    EdgeElement putEdge(VertexElement to, Schema.EdgeLabel type);
    void deleteEdge(Direction direction, Schema.EdgeLabel edgeLabel, VertexElement... targets);

    Stream<EdgeElement> getEdgesOfType(Direction direction, Schema.EdgeLabel label);

    /**
     * Retrieve this vertex as a Shard object
     * @return
     */
    Shard asShard();

    /**
     * Retrieve the current shard connected to this vertex
     * (currently only applies to Types)
     * @return
     */
    Shard currentShard();
    /**
     * Create a new vertex that is a shard and connect it to the owning vertex (this vertex)
     * @return
     */
    Shard shard();

    /**
     * @return all current shards of this vertex
     */
    Stream<Shard> shards();



    // methods that should probably be removed from the interface
    Stream<EdgeElement> roleCastingsEdges(Type type, Set<Integer> allowedRoleTypeIds);
    boolean rolePlayerEdgeExists(String startVertexId, RelationType type, Role role, String endVertexId);
    Stream<VertexElement> getShortcutNeighbors(Set<Integer> ownerRoleIds, Set<Integer> valueRoleIds,
                                                      boolean ownerToValueOrdering);
    Stream<EdgeElement> edgeRelationsConnectedToInstancesOfType(LabelId edgeInstanceLabelId);

    Stream<VertexElement> reifiedRelations(Role... roles);
}

