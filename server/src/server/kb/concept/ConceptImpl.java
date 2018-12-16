/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.server.kb.concept;

import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.internal.Schema;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.kb.cache.Cache;
import grakn.core.server.kb.cache.CacheOwner;
import grakn.core.server.kb.cache.Cacheable;
import grakn.core.server.kb.structure.EdgeElement;
import grakn.core.server.kb.structure.Shard;
import grakn.core.server.kb.structure.VertexElement;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;


/**
 * The base concept implementation.
 * A concept which can represent anything in the graph which wraps a TinkerPop vertex.
 * This class forms the basis of assuring the graph follows the Grakn object model.
 */
public abstract class ConceptImpl implements Concept, ConceptVertex, CacheOwner {
    private final Set<Cache> registeredCaches = new HashSet<>();
    //WARNING: DO not flush the current shard into the central cache. It is not safe to do so in a concurrent environment
    private final Cache<Shard> currentShard = Cache.createTxCache(this, Cacheable.shard(), () -> {
        String currentShardId = vertex().property(Schema.VertexProperty.CURRENT_SHARD);
        Vertex shardVertex = vertex().tx().getTinkerTraversal().V().has(Schema.VertexProperty.ID.name(), currentShardId).next();
        return vertex().tx().factory().buildShard(shardVertex);
    });
    private final Cache<Long> shardCount = Cache.createSessionCache(this, Cacheable.number(), () -> shards().count());
    private final Cache<ConceptId> conceptId = Cache.createPersistentCache(this, Cacheable.conceptId(), () -> ConceptId.of(vertex().property(Schema.VertexProperty.ID)));
    private final VertexElement vertexElement;

    ConceptImpl(VertexElement vertexElement) {
        this.vertexElement = vertexElement;
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
     * @throws TransactionException Throws an exception if the node has any edges attached to it.
     */
    @Override
    public void delete() throws TransactionException {
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
        vertex().tx().cache().remove(this);
        vertex().delete();
    }

    @Override
    public Collection<Cache> caches() {
        return registeredCaches;
    }

    /**
     * @param direction the direction of the neigouring concept to get
     * @param label     The edge label to traverse
     * @return The neighbouring concepts found by traversing edges of a specific type
     */
    <X extends Concept> Stream<X> neighbours(Direction direction, Schema.EdgeLabel label) {
        switch (direction) {
            case BOTH:
                return vertex().getEdgesOfType(direction, label).
                        flatMap(edge -> Stream.<X>of(
                                vertex().tx().factory().buildConcept(edge.source()),
                                vertex().tx().factory().buildConcept(edge.target()))
                        );
            case IN:
                return vertex().getEdgesOfType(direction, label).map(edge -> vertex().tx().factory().buildConcept(edge.source()));
            case OUT:
                return vertex().getEdgesOfType(direction, label).map(edge -> vertex().tx().factory().buildConcept(edge.target()));
            default:
                throw TransactionException.invalidDirection(direction);
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
        if (vertex().tx().isValidElement(vertex().element())) {
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
        VertexElement shardVertex = vertex().tx().addVertexElement(Schema.BaseType.SHARD);
        Shard shard = vertex().tx().factory().buildShard(this, shardVertex);
        vertex().property(Schema.VertexProperty.CURRENT_SHARD, shard.id());
        currentShard.set(shard);

        //Updated the cached shard count if needed
        if (shardCount.isPresent()) {
            shardCount.set(shardCount() + 1);
        }
    }

    public Stream<Shard> shards() {
        return vertex().getEdgesOfType(Direction.IN, Schema.EdgeLabel.SHARD).
                map(EdgeElement::source).
                map(edge -> vertex().tx().factory().buildShard(edge));
    }

    public Long shardCount() {
        return shardCount.get();
    }

    public Shard currentShard() {
        return currentShard.get();
    }

}
