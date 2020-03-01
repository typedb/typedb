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

package grakn.core.kb.concept.structure;

import grakn.core.core.Schema;
import grakn.core.kb.concept.api.LabelId;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Type;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

public interface VertexElement extends AbstractElement<Vertex> {
    EdgeElement addEdge(VertexElement to, Schema.EdgeLabel type);
    EdgeElement putEdge(VertexElement to, Schema.EdgeLabel type);
    void deleteEdge(Direction direction, Schema.EdgeLabel edgeLabel, VertexElement... targets);

    /**
     * Sets a property which cannot be mutated
     *
     * @param property   The key of the immutable property to mutate
     * @param newValue   The new value to put on the property (if the property is not set)
     * @param foundValue The current value of the property
     * @param converter  Helper method to ensure data is persisted in the correct format
     */
    <X> void propertyImmutable(Schema.VertexProperty property, X newValue, @Nullable X foundValue, Function<X, Object> converter);
    <X> void propertyImmutable(Schema.VertexProperty property, X newValue, X foundValue);

    void property(Schema.VertexProperty key, Object value);
    <X> X property(Schema.VertexProperty key);
    Boolean propertyBoolean(Schema.VertexProperty key);

    /**
     * Sets the value of a property with the added restriction that no other vertex can have that property.
     *
     * @param key   The key of the unique property to mutate
     * @param value The new value of the unique property
     */
    void propertyUnique(Schema.VertexProperty key, String value);


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

