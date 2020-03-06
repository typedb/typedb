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

package grakn.core.concept.structure;

import grakn.core.core.Schema;
import grakn.core.kb.concept.api.LabelId;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.structure.EdgeElement;
import grakn.core.kb.concept.structure.GraknElementException;
import grakn.core.kb.concept.structure.PropertyNotUniqueException;
import grakn.core.kb.concept.structure.Shard;
import grakn.core.kb.concept.structure.VertexElement;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toSet;

/**
 * Represent a Vertex in a TransactionOLTP
 * Wraps a tinkerpop Vertex constraining it to the Grakn Object Model.
 * This is used to wrap common functionality between exposed Concept and unexposed
 * internal vertices.
 */
public class VertexElementImpl extends AbstractElementImpl<Vertex> implements VertexElement {

    public VertexElementImpl(ElementFactory elementFactory, Vertex element) {
        super(elementFactory, element);
    }

    /**
     * @param direction The direction of the edges to retrieve
     * @param label     The type of the edges to retrieve
     * @return A collection of edges from this concept in a particular direction of a specific type
     */
    public Stream<EdgeElement> getEdgesOfType(Direction direction, Schema.EdgeLabel label) {
        Iterable<Edge> iterable = () -> element().edges(direction, label.getLabel());
        return StreamSupport.stream(iterable.spliterator(), false)
                .filter(edge -> ElementUtils.isValidElement(edge)) // filter out deleted but cached available edges
                .map(edge -> elementFactory.buildEdgeElement(edge));
    }

    /**
     * @param to   the target VertexElement
     * @param type the type of the edge to create
     * @return The edge created
     */
    public EdgeElement addEdge(VertexElement to, Schema.EdgeLabel type) {
        Edge newEdge = element().addEdge(type.getLabel(), to.element());
        return elementFactory.buildEdgeElement(newEdge);
    }

    /**
     * @param to   the target VertexElement
     * @param type the type of the edge to create
     */
    public EdgeElement putEdge(VertexElement to, Schema.EdgeLabel type) {
        EdgeElement existingEdge = elementFactory.edgeBetweenVertices(id().toString(), to.id().toString(), type);

        if (existingEdge == null) {
            return addEdge(to, type);
        } else {
            return existingEdge;
        }
    }

    /**
     * Deletes all the edges of a specific Schema.EdgeLabel to or from a specific set of targets.
     * If no targets are provided then all the edges of the specified type are deleted
     *
     * @param direction The direction of the edges to delete
     * @param label     The edge label to delete
     * @param targets   An optional set of targets to delete edges from
     */
    public void deleteEdge(Direction direction, Schema.EdgeLabel label, VertexElement... targets) {
        Iterator<Edge> edges = element().edges(direction, label.getLabel());
        if (targets.length == 0) {
            edges.forEachRemaining(Edge::remove);
        } else {
            Set<Vertex> verticesToDelete = Arrays.stream(targets).map(VertexElement::element).collect(Collectors.toSet());
            edges.forEachRemaining(edge -> {
                boolean delete = false;
                switch (direction) {
                    case BOTH:
                        delete = verticesToDelete.contains(edge.inVertex()) || verticesToDelete.contains(edge.outVertex());
                        break;
                    case IN:
                        delete = verticesToDelete.contains(edge.outVertex());
                        break;
                    case OUT:
                        delete = verticesToDelete.contains(edge.inVertex());
                        break;
                }

                if (delete) edge.remove();
            });
        }
    }


    /**
     * @param key The key of the non-unique property to retrieve
     * @return The value stored in the property
     */
    @Override
    @Nullable
    public <X> X property(Schema.VertexProperty key) {
        Property<X> property = element().property(key.name());
        if (property != null && property.isPresent()) {
            return property.value();
        }
        return null;
    }

    @Override
    public Boolean propertyBoolean(Schema.VertexProperty key) {
        Boolean value = property(key);
        if (value == null) return false;
        return value;
    }

    /**
     * @param key   The key of the property to mutate
     * @param value The value to commit into the property
     */
    @Override
    public void property(Schema.VertexProperty key, Object value) {
        element().property(key.name()).remove();
        if (value != null) {
            element().property(key.name(), value);
        }
    }

    /**
     * Sets a property which cannot be mutated
     *
     * @param property   The key of the immutable property to mutate
     * @param newValue   The new value to put on the property (if the property is not set)
     * @param foundValue The current value of the property
     * @param converter  Helper method to ensure data is persisted in the correct format
     */
    @Override
    public <X> void propertyImmutable(Schema.VertexProperty property, X newValue, @Nullable X foundValue, Function<X, Object> converter) {
        Objects.requireNonNull(property);

        if (foundValue == null) {
            property(property, converter.apply(newValue));

        } else if (!foundValue.equals(newValue)) {
            throw GraknElementException.immutableProperty(foundValue, newValue, property);
        }
    }

    @Override
    public <X> void propertyImmutable(Schema.VertexProperty property, X newValue, X foundValue) {
        propertyImmutable(property, newValue, foundValue, Function.identity());
    }

    /**
     * Sets the value of a property with the added restriction that no other vertex can have that property.
     *
     * @param key   The key of the unique property to mutate
     * @param value The new value of the unique property
     */
    @Override
    public void propertyUnique(Schema.VertexProperty key, String value) {
        Iterator<VertexElement> verticesWithProperty = elementFactory.getVerticesWithProperty(key, value).iterator();

        if (verticesWithProperty.hasNext()) {
            Vertex vertex = verticesWithProperty.next().element();
            if (!vertex.equals(element()) || verticesWithProperty.hasNext()) {
                if (verticesWithProperty.hasNext()) vertex = verticesWithProperty.next().element();
                throw PropertyNotUniqueException.cannotChangeProperty(element(), vertex, key, value);
            }
        }

        property(key, value);
    }

    public Shard asShard() {
        return elementFactory.getShard(this);
    }

    public Shard currentShard() {
        Object currentShardId = property(Schema.VertexProperty.CURRENT_SHARD);
        Vertex shardVertex = elementFactory.getVertexWithId(currentShardId.toString());
        return elementFactory.getShard(shardVertex);
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Vertex [").append(id()).append("] /n");
        element().properties().forEachRemaining(
                p -> stringBuilder.append("Property [").append(p.key()).append("] value [").append(p.value()).append("] /n"));
        return stringBuilder.toString();
    }

    /**
     * Create a new vertex that is a shard and connect it to the owning vertex (this vertex)
     */
    public Shard shard() {
        VertexElement shardVertex = elementFactory.addVertexElement(Schema.BaseType.SHARD);
        Shard shard = elementFactory.createShard(this, shardVertex);
        return shard;
    }

    public Stream<Shard> shards() {
        return getEdgesOfType(Direction.IN, Schema.EdgeLabel.SHARD)
                .map(EdgeElement::source)
                .map(vertexElement -> elementFactory.getShard(vertexElement));
    }

    public Stream<EdgeElement> roleCastingsEdges(Type type, Set<Integer> allowedRoleTypeIds) {
        return elementFactory.rolePlayerEdges(id().toString(), type, allowedRoleTypeIds);

    }

    public boolean rolePlayerEdgeExists(String startVertexId, RelationType type, Role role, String endVertexId) {
        return elementFactory.rolePlayerEdgeExists(startVertexId, type, role, endVertexId);
    }

    public Stream<VertexElement> getShortcutNeighbors(Set<Integer> ownerRoleIds, Set<Integer> valueRoleIds,
                                                      boolean ownerToValueOrdering) {
        return elementFactory.shortcutNeighbors(id().toString(), ownerRoleIds, valueRoleIds, ownerToValueOrdering);
    }

    public Stream<EdgeElement> edgeRelationsConnectedToInstancesOfType(LabelId edgeInstanceLabelId) {
        return elementFactory.edgeRelationsConnectedToInstancesOfType(id().toString(), edgeInstanceLabelId);
    }

    @Override
    public Stream<VertexElement> reifiedRelations(Role[] roles) {
        if (roles.length == 0) {
            return elementFactory.inFromSourceId(id().toString(), Schema.EdgeLabel.ROLE_PLAYER);
        } else {
            Set<Integer> roleTypesIds = Arrays.stream(roles).map(r -> r.labelId().getValue()).collect(toSet());
            return elementFactory.inFromSourceIdWithProperty(id().toString(), Schema.EdgeLabel.ROLE_PLAYER,
                    Schema.EdgeProperty.ROLE_LABEL_ID, roleTypesIds);
        }
    }
}
