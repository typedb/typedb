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

package ai.grakn.graph.internal;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;
import java.util.stream.Collectors;
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
abstract class ConceptImpl implements Concept, ConceptVertex, ContainsTxCache {
    private final Cache<ConceptId> conceptId = new Cache<>(() -> ConceptId.of(vertex().property(Schema.VertexProperty.ID)), Cacheable.conceptId());
    private final VertexElement vertexElement;

    @SuppressWarnings("unchecked")
    <X extends  Concept> X getThis(){
        return (X) this;
    }

    ConceptImpl(VertexElement vertexElement){
        this.vertexElement = vertexElement;
    }

    @Override
    public VertexElement vertex() {
        return vertexElement;
    }

    /**
     * Deletes the concept.
     * @throws GraphOperationException Throws an exception if the node has any edges attached to it.
     */
    @Override
    public void delete() throws GraphOperationException {
        deleteNode();
    }

    /**
     * Deletes the node and adds it neighbours for validation
     */
    void deleteNode(){
        vertex().graph().txCache().remove(this);
        vertex().delete();
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
                        flatMap(edge -> Stream.of(
                                vertex().graph().factory().buildConcept(edge.source()),
                                vertex().graph().factory().buildConcept(edge.target())
                        ));
            case IN:
                return vertex().getEdgesOfType(direction, label).map(edge ->
                        vertex().graph().factory().buildConcept(edge.source())
                );
            case OUT:
                return  vertex().getEdgesOfType(direction, label).map(edge ->
                        vertex().graph().factory().buildConcept(edge.target())
                );
            default:
                throw GraphOperationException.invalidDirection(direction);
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
    Schema.BaseType baseType(){
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

    @Override public int hashCode() {
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
        try {
            vertex().graph().validVertex(vertex().element());
            return innerToString();
        } catch (RuntimeException e){
            // Vertex is broken somehow. Most likely deleted.
            return "Id [" + getId() + "]";
        }
    }

    protected String innerToString() {
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
    void createShard(){
        VertexElement shardVertex = vertex().graph().addVertex(Schema.BaseType.SHARD);
        Shard shard = vertex().graph().factory().buildShard(this, shardVertex);
        vertex().property(Schema.VertexProperty.CURRENT_SHARD, shard.id());
    }

    Set<Shard> shards(){
        return vertex().getEdgesOfType(Direction.IN, Schema.EdgeLabel.SHARD).map(edge ->
                vertex().graph().factory().buildShard(edge.source())).collect(Collectors.toSet());
    }

    Shard currentShard(){
        String currentShardId = vertex().property(Schema.VertexProperty.CURRENT_SHARD);
        Vertex shardVertex = vertex().graph().getTinkerTraversal().V().has(Schema.VertexProperty.ID.name(), currentShardId).next();
        return vertex().graph().factory().buildShard(shardVertex);
    }

    long getShardCount(){
        Long value = vertex().property(Schema.VertexProperty.SHARD_COUNT);
        if(value == null) return 0L;
        return value;
    }

    void setShardCount(Long instanceCount){
        vertex().property(Schema.VertexProperty.SHARD_COUNT, instanceCount);
    }
}
