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

import grakn.core.concept.cache.ConceptCache;
import grakn.core.concept.structure.ElementUtils;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.concept.manager.ConceptNotificationChannel;
import grakn.core.kb.concept.structure.EdgeElement;
import grakn.core.kb.concept.structure.GraknElementException;
import grakn.core.kb.concept.structure.Shard;
import grakn.core.kb.concept.structure.VertexElement;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.stream.Stream;

/**
 * The base concept implementation.
 * A concept which can represent anything in the graph which wraps a TinkerPop vertex.
 * This class forms the basis of assuring the graph follows the Grakn object model.
 */
public abstract class ConceptImpl implements Concept, ConceptVertex {
    private final VertexElement vertexElement;
    final ConceptManager conceptManager;
    final ConceptNotificationChannel conceptNotificationChannel;

    private final ConceptCache<Shard> currentShard;
    private final ConceptCache<Long> shardCount;
    private final ConceptCache<ConceptId> conceptId;

    ConceptImpl(VertexElement vertexElement, ConceptManager conceptManager, ConceptNotificationChannel conceptNotificationChannel) {
        this.vertexElement = vertexElement;
        this.conceptManager = conceptManager;
        this.currentShard = new ConceptCache<>(() -> conceptManager.getShardWithLock(vertex().element().id().toString()));
        this.shardCount = new ConceptCache<>(() -> shards().count());
        this.conceptId = new ConceptCache<>(() -> Schema.conceptId(vertex().element()));
        this.conceptNotificationChannel = conceptNotificationChannel;
    }

    @Override
    public VertexElement vertex() {
        return vertexElement;
    }

    @SuppressWarnings("unchecked")
    <X extends Concept> X getThis() {
        return (X) this;
    }

    /**
     * Deletes the concept.
     *
     */
    @Override
    public void delete() {
        deleteNode();
    }

    @Override
    public boolean isDeleted() {
        return vertex().isDeleted();
    }

    /**
     * Deletes the node and adds it neighbours for validation
     */
    public void deleteNode() {
        // TODO write cache tests to ensure that this is safe to remove
//        conceptNotificationChannel.transactionCache().remove(this);
        vertex().delete();
    }

    /**
     * @param direction the direction of the neighbouring concept to get
     * @param label     The edge label to traverse
     * @return The neighbouring concepts found by traversing edges of a specific type
     */
    public <X extends Concept> Stream<X> neighbours(Direction direction, Schema.EdgeLabel label) {
        switch (direction) {
            case BOTH:
                return vertex().getEdgesOfType(direction, label).
                        flatMap(edge -> Stream.of(
                                conceptManager.buildConcept(edge.source()),
                                conceptManager.buildConcept(edge.target()))
                        );
            case IN:
                return vertex().getEdgesOfType(direction, label).map(edge -> conceptManager.buildConcept(edge.source()));
            case OUT:
                return vertex().getEdgesOfType(direction, label).map(edge -> conceptManager.buildConcept(edge.target()));
            default:
                throw GraknElementException.invalidDirection(direction);
        }
    }

    EdgeElement putEdge(ConceptVertex to, Schema.EdgeLabel label) {
        return vertex().putEdge(to.vertex(), label);
    }

    EdgeElement addEdge(ConceptVertex to, Schema.EdgeLabel label) {
        return vertex().addEdge(to.vertex(), label);
    }

    void deleteEdge(Direction direction, Schema.EdgeLabel label, Concept... to) {
        if (to.length == 0) {
            vertex().deleteEdge(direction, label);
        } else {
            VertexElement[] targets = new VertexElement[to.length];
            for (int i = 0; i < to.length; i++) {
                targets[i] = ((ConceptImpl) to[i]).vertex();
            }
            vertex().deleteEdge(direction, label, targets);
        }
    }

    /**
     * @return The base type of this concept which helps us identify the concept
     */
    public Schema.BaseType baseType() {
        return Schema.BaseType.valueOf(vertex().label());
    }

    /**
     * @return A string representing the concept's unique id.
     */
    @Override
    public ConceptId id() {
        return conceptId.get();
    }

    @Override
    public int hashCode() {
        return id().hashCode(); //Note: This means that concepts across different transactions will be equivalent.
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        ConceptImpl concept = (ConceptImpl) object;

        //based on id because vertex comparisons are equivalent
        return id().equals(concept.id());
    }

    @Override
    public final String toString() {
        if (ElementUtils.isValidElement(vertex().element())) {
            return innerToString();
        } else {
            // Vertex has been deleted so all we can do is print the id
            return "Id [" + vertex().id() + "]";
        }
    }

    String innerToString() {
        String message = "Base Type [" + baseType() + "] ";
        if (id() != null) {
            message = message + "- Id [" + id() + "] ";
        }

        return message;
    }

    //----------------------------------- Sharding Functionality
    public void createShard() {
        Shard shard = vertex().shard();

        //store current shard id as a property of the type
        vertex().property(Schema.VertexProperty.CURRENT_SHARD, shard.id());
        currentShard.set(shard);

        //Updated the cached shard count if needed
        if (shardCount.isCached()) {
            shardCount.set(shardCount() + 1);
        }
    }

    public Stream<Shard> shards() {
        return vertex().shards();
    }

    public Long shardCount() {
        return shardCount.get();
    }

    public Shard currentShard() {
        return currentShard.get();
    }

}
