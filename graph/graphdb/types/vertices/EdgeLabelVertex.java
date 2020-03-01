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

package grakn.core.graph.graphdb.types.vertices;

import grakn.core.graph.core.Connection;
import grakn.core.graph.core.EdgeLabel;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import grakn.core.graph.graphdb.types.TypeDefinitionCategory;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class EdgeLabelVertex extends RelationTypeVertex implements EdgeLabel {

    public EdgeLabelVertex(StandardJanusGraphTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    @Override
    public boolean isDirected() {
        return isUnidirected(Direction.BOTH);
    }

    @Override
    public boolean isUnidirected() {
        return isUnidirected(Direction.OUT);

    }

    @Override
    public Collection<PropertyKey> mappedProperties() {
        return StreamSupport.stream(getRelated(TypeDefinitionCategory.PROPERTY_KEY_EDGE, Direction.OUT).spliterator(), false)
            .map(entry -> (PropertyKey) entry.getSchemaType())
            .collect(Collectors.toList());
    }

    @Override
    public Collection<Connection> mappedConnections() {
        String name = name();
        return StreamSupport.stream(getRelated(TypeDefinitionCategory.UPDATE_CONNECTION_EDGE, Direction.OUT).spliterator(), false)
            .map(entry -> (JanusGraphSchemaVertex) entry.getSchemaType())
            .flatMap(s -> StreamSupport.stream(s.getEdges(TypeDefinitionCategory.CONNECTION_EDGE, Direction.OUT).spliterator(), false))
            .map(Connection::new)
            .filter(s -> s.getEdgeLabel().equals(name))
            .collect(Collectors.toList());
    }

    @Override
    public boolean isUnidirected(Direction dir) {
        return getDefinition().getValue(TypeDefinitionCategory.UNIDIRECTIONAL, Direction.class)==dir;
    }

    @Override
    public final boolean isPropertyKey() {
        return false;
    }

    @Override
    public final boolean isEdgeLabel() {
        return true;
    }
}
