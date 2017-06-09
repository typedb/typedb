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
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.exception.PropertyNotUniqueException;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


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
 * @param <T> The leaf interface of the object concept.
 *           For example an {@link EntityType}, {@link Entity}, {@link RelationType} etc . . .
 */
abstract class ConceptImpl<T extends Concept> extends AbstractElement<Vertex> implements Concept {
    private ElementCache<Boolean> cachedIsShard = new ElementCache<>(() -> getPropertyBoolean(Schema.ConceptProperty.IS_SHARD));

    @SuppressWarnings("unchecked")
    T getThis(){
        return (T) this;
    }

    ConceptImpl(AbstractGraknGraph graknGraph, Vertex v){
        super(graknGraph, v);
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
     *
     * @param key The key of the unique property to mutate
     * @param value The new value of the unique property
     * @return The concept itself casted to the correct interface itself
     */
    T setUniqueProperty(Schema.ConceptProperty key, String value){
        if(!getGraknGraph().isBatchGraph()) {
            Concept fetchedConcept = getGraknGraph().getConcept(key, value);
            if (fetchedConcept != null) throw PropertyNotUniqueException.cannotChangeProperty(this, fetchedConcept, key, value);
        }

        return setProperty(key, value);
    }

    /**
     * Deletes the node and adds it neighbours for validation
     */
    void deleteNode(){
        getGraknGraph().getTxCache().remove(this);
        // delete node
        getElement().remove();
    }

    /**
     *
     * @param edgeType The edge label to traverse
     * @return The neighbouring concepts found by traversing outgoing edges of a specific type
     */
    <X extends Concept> Stream<X> getOutgoingNeighbours(Schema.EdgeLabel edgeType){
        return getEdgesOfType(Direction.OUT, edgeType).map(EdgeElement::getTarget);
    }

    /**
     *
     * @param edgeType The edge label to traverse
     * @return The neighbouring concepts found by traversing incoming edges of a specific type
     */
    <X extends Concept> Stream<X> getIncomingNeighbours(Schema.EdgeLabel edgeType){
        return getEdgesOfType(Direction.IN, edgeType).map(EdgeElement::getSource);
    }

    /**
     *
     * @param key The key of the non-unique property to mutate
     * @param value The value to commit into the property
     * @return The concept itself casted to the correct interface
     */
    T setProperty(Schema.ConceptProperty key, Object value){
        setProperty(key.name(), value);
        return getThis();
    }

    /**
     *
     * @param key The key of the non-unique property to retrieve
     * @return The value stored in the property
     */
    public <X> X getProperty(Schema.ConceptProperty key){
        return getProperty(key.name());
    }
    Boolean getPropertyBoolean(Schema.ConceptProperty key){
        return getPropertyBoolean(key.name());
    }

    /**
     *
     * @return The base ttpe of this concept which helps us identify the concept
     */
    Schema.BaseType getBaseType(){
        return Schema.BaseType.valueOf(getElement().label());
    }

    /**
     *
     * @return A string representing the concept's unique id.
     */
    @Override
    public ConceptId getId(){
        return ConceptId.of(getElementId());
    }

    /**
     *
     * @param direction The direction of the edges to retrieve
     * @param type The type of the edges to retrieve
     * @return A collection of edges from this concept in a particular direction of a specific type
     */
    Stream<EdgeElement> getEdgesOfType(Direction direction, Schema.EdgeLabel type){
        Iterable<Edge> iterable = () -> getElement().edges(direction, type.getLabel());
        return StreamSupport.stream(iterable.spliterator(), false).
                map(edge -> getGraknGraph().getElementFactory().buildEdge(edge));
    }

    //--------- Create Links -------//
    /**
     *  @param to the target concept
     * @param type the type of the edge to create
     */
    EdgeElement putEdge(Concept to, Schema.EdgeLabel type){
        ConceptImpl toConcept = (ConceptImpl) to;
        GraphTraversal<Vertex, Edge> traversal = getGraknGraph().getTinkerPopGraph().traversal().V(getId().getRawValue()).outE(type.getLabel()).as("edge").otherV().hasId(toConcept.getId().getRawValue()).select("edge");
        if(!traversal.hasNext()) {
            return addEdge(toConcept, type);
        } else {
            return getGraknGraph().getElementFactory().buildEdge(traversal.next());
        }
    }

    /**
     *
     * @param toConcept the target concept
     * @param type the type of the edge to create
     * @return The edge created
     */
    EdgeElement addEdge(ConceptImpl toConcept, Schema.EdgeLabel type) {
        return getGraknGraph().getElementFactory().buildEdge(toConcept.addEdgeFrom(getElement(), type.getLabel()));
    }

    /**
     *
     * @param direction The direction of the edges to retrieve
     * @param type The type of the edges to retrieve
     */
    void deleteEdges(Direction direction, Schema.EdgeLabel type){
        getElement().edges(direction, type.getLabel()).forEachRemaining(Edge::remove);
    }

    /**
     * Deletes an edge of a specific type going to a specific concept
     * @param type The type of the edge
     * @param toConcept The target concept
     */
    void deleteEdgeTo(Schema.EdgeLabel type, Concept toConcept){
        GraphTraversal<Vertex, Edge> traversal = getGraknGraph().getTinkerPopGraph().traversal().V(getId().getRawValue()).
                outE(type.getLabel()).as("edge").otherV().hasId(toConcept.getId().getRawValue()).select("edge");
        if(traversal.hasNext()) {
            traversal.next().remove();
        }
    }

    private Edge addEdgeFrom(Vertex fromVertex, String type) {
        return fromVertex.addEdge(type, getElement());
    }

    /**
     *
     * @return The hash code of the underlying vertex
     */
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
        try {
            getGraknGraph().validVertex(getElement());
            return innerToString();
        } catch (RuntimeException e){
            // Vertex is broken somehow. Most likely deleted.
            return "Id [" + getId() + "]";
        }
    }

    protected String innerToString() {
        String message = "Base Type [" + getBaseType() + "] ";
        if(getId() != null) {
            message = message + "- Id [" + getId() + "] ";
        }

        return message;
    }

    <X> void setImmutableProperty(Schema.ConceptProperty conceptProperty, X newValue, X foundValue, Function<X, Object> converter){
        if(newValue == null){
            throw GraphOperationException.settingNullProperty(conceptProperty);
        }

        if(foundValue != null){
            if(!foundValue.equals(newValue)){
                throw GraphOperationException.immutableProperty(foundValue, newValue, this, conceptProperty);
            }
        } else {
            setProperty(conceptProperty, converter.apply(newValue));
        }
    }
    
    @Override
    public int compareTo(Concept o) {
        return this.getId().compareTo(o.getId());
    }

    //----------------------------------- Sharding Functionality
    T createShard(){
        Vertex shardVertex = getGraknGraph().addVertex(getBaseType());
        shardVertex.addEdge(Schema.EdgeLabel.SHARD.getLabel(), getElement());

        ConceptImpl shardConcept = getGraknGraph().buildConcept(shardVertex);
        shardConcept.isShard(true);
        setProperty(Schema.ConceptProperty.CURRENT_SHARD, shardConcept.getId().getValue());

        //noinspection unchecked
        return (T) shardConcept;
    }

    Set<T> shards(){
        return this.<T>getIncomingNeighbours(Schema.EdgeLabel.SHARD).collect(Collectors.toSet());
    }

    //TODO: Return implementation rather than interface
    T currentShard(){
        String currentShardId = getProperty(Schema.ConceptProperty.CURRENT_SHARD);
        return getGraknGraph().getConcept(ConceptId.of(currentShardId));
    }

    boolean isShard(){
        return cachedIsShard.get();
    }

    void isShard(Boolean isShard){
        if(isShard) setProperty(Schema.ConceptProperty.IS_SHARD, isShard);
        cachedIsShard.set(isShard);
    }

    long getShardCount(){
        Long value = getProperty(Schema.ConceptProperty.SHARD_COUNT);
        if(value == null) return 0L;
        return value;
    }

    void setShardCount(Long instanceCount){
        setProperty(Schema.ConceptProperty.SHARD_COUNT, instanceCount);
    }
}
