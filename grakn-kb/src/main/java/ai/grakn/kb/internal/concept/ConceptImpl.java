/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.kb.internal.concept;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.kb.internal.cache.Cache;
import ai.grakn.kb.internal.cache.CacheOwner;
import ai.grakn.kb.internal.cache.Cacheable;
import ai.grakn.kb.internal.structure.EdgeElement;
import ai.grakn.kb.internal.structure.Shard;
import ai.grakn.kb.internal.structure.VertexElement;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;


/**
 * <p>
 *     The base concept implementation.
 * </p>
 *
 * <p>
 *     A concept which can represent anything in the graph which wraps a tinkerpop {@link Vertex}.
 *     This class forms the basis of assuring the graph follows the Grakn object model.
 * </p>
 *
 * @author fppt
 *
 */
public abstract class ConceptImpl implements Concept, ConceptVertex, CacheOwner{
    private final Set<Cache> registeredCaches = new HashSet<>();
    //WARNING: DO not flush the current shard into the central cache. It is not safe to do so in a concurrent environment
    private final Cache<Shard> currentShard = Cache.createTxCache(this, Cacheable.shard(), () -> {
        String currentShardId = vertex().property(Schema.VertexProperty.CURRENT_SHARD);
        Vertex shardVertex = vertex().tx().getTinkerTraversal().V().has(Schema.VertexProperty.ID.name(), currentShardId).next();
        Optional<Shard> shard = vertex().tx().factory().buildShard(shardVertex);
        return shard.orElseThrow(() -> GraknTxOperationException.missingShard(getId()));
    });
    private final Cache<Long> shardCount = Cache.createSessionCache(this, Cacheable.number(), () -> shards().count());
    private final Cache<ConceptId> conceptId = Cache.createPersistentCache(this, Cacheable.conceptId(), () -> ConceptId.of(vertex().property(Schema.VertexProperty.ID)));
    private final VertexElement vertexElement;

    ConceptImpl(VertexElement vertexElement){
        this.vertexElement = vertexElement;
    }

    @Override
    public VertexElement vertex() {
        return vertexElement;
    }

    @SuppressWarnings("unchecked")
    <X extends  Concept> X getThis(){
        return (X) this;
    }

    /**
     * Deletes the concept.
     * @throws GraknTxOperationException Throws an exception if the node has any edges attached to it.
     */
    @Override
    public void delete() throws GraknTxOperationException {
        deleteNode();
    }

    @Override
    public boolean isDeleted() {
        return vertex().isDeleted();
    }

    /**
     * Deletes the node and adds it neighbours for validation
     */
    public void deleteNode(){
        vertex().tx().txCache().remove(this);
        vertex().delete();
    }

    @Override
    public Collection<Cache> caches(){
        return registeredCaches;
    }

    /**
     *
     * @param direction the direction of the neigouring concept to get
     * @param label The edge label to traverse
     * @return The neighbouring concepts found by traversing edges of a specific type
     */
    <X extends Concept> Stream<X> neighbours(Direction direction, Schema.EdgeLabel label){
        switch (direction){
            case BOTH:
                return vertex().getEdgesOfType(direction, label).
                        flatMap(edge -> Stream.<Optional<X>>of(
                                edge.source().flatMap(source -> vertex().tx().factory().buildConcept(source)),
                                edge.target().flatMap(target -> vertex().tx().factory().buildConcept(target))
                        )).flatMap(CommonUtil::optionalToStream);
            case IN:
                return vertex().getEdgesOfType(direction, label).flatMap(edge -> {
                    Optional<X> optional = edge.source().flatMap(source -> vertex().tx().factory().buildConcept(source));
                    return CommonUtil.optionalToStream(optional);
                });
            case OUT:
                return  vertex().getEdgesOfType(direction, label).flatMap(edge -> {
                    Optional<X> optional = edge.target().flatMap(target -> vertex().tx().factory().buildConcept(target));
                    return CommonUtil.optionalToStream(optional);
                });
            default:
                throw GraknTxOperationException.invalidDirection(direction);
        }
    }

    EdgeElement putEdge(ConceptVertex to, Schema.EdgeLabel label){
        return vertex().putEdge(to.vertex(), label);
    }

    EdgeElement addEdge(ConceptVertex to, Schema.EdgeLabel label){
        return vertex().addEdge(to.vertex(), label);
    }

    void deleteEdge(Direction direction, Schema.EdgeLabel label, Concept... to) {
        if (to.length == 0) {
            vertex().deleteEdge(direction, label);
        } else{
            VertexElement[] targets = new VertexElement[to.length];
            for (int i = 0; i < to.length; i++) {
                targets[i] = ((ConceptImpl)to[i]).vertex();
            }
            vertex().deleteEdge(direction, label, targets);
        }
    }

    /**
     *
     * @return The base type of this concept which helps us identify the concept
     */
    public Schema.BaseType baseType(){
        return Schema.BaseType.valueOf(vertex().label());
    }

    /**
     *
     * @return A string representing the concept's unique id.
     */
    @Override
    public ConceptId getId(){
        return conceptId.get();
    }

    @Override
    public int hashCode() {
        return getId().hashCode(); //Note: This means that concepts across different transactions will be equivalent.
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        ConceptImpl concept = (ConceptImpl) object;

        //based on id because vertex comparisons are equivalent
        return getId().equals(concept.getId());
    }

    @Override
    public final String toString(){
        if (vertex().tx().validElement(vertex().element())) {
            return innerToString();
        } else {
            // Vertex is broken somehow. Most likely deleted.
            return "Id [" + vertex().id() + "]";
        }
    }

    String innerToString() {
        String message = "Base Type [" + baseType() + "] ";
        if(getId() != null) {
            message = message + "- Id [" + getId() + "] ";
        }

        return message;
    }

    @Override
    public int compareTo(Concept o) {
        return this.getId().compareTo(o.getId());
    }

    //----------------------------------- Sharding Functionality
    public void createShard(){
        VertexElement shardVertex = vertex().tx().addVertexElement(Schema.BaseType.SHARD);
        Shard shard = vertex().tx().factory().buildShard(this, shardVertex);
        vertex().property(Schema.VertexProperty.CURRENT_SHARD, shard.id());
        currentShard.set(shard);

        //Updated the cached shard count if needed
        if(shardCount.isPresent()){
            shardCount.set(shardCount() + 1);
        }
    }

    public Stream<Shard> shards(){
        return vertex().getEdgesOfType(Direction.IN, Schema.EdgeLabel.SHARD).
                map(EdgeElement::source).
                flatMap(CommonUtil::optionalToStream).
                map(edge -> vertex().tx().factory().buildShard(edge));
    }

    public Long shardCount(){
        return shardCount.get();
    }

    public Shard currentShard(){
        return currentShard.get();
    }

}
