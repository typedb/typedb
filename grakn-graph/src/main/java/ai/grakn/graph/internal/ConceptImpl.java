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
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import ai.grakn.exception.ConceptException;
import ai.grakn.exception.ConceptNotUniqueException;
import ai.grakn.exception.InvalidConceptTypeException;
import ai.grakn.exception.InvalidConceptValueException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.function.Function;
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
abstract class ConceptImpl<T extends Concept> implements Concept {
    private final AbstractGraknGraph graknGraph;
    private final ConceptId conceptId;
    private final Vertex vertex;

    @SuppressWarnings("unchecked")
    T getThis(){
        return (T) this;
    }

    ConceptImpl(AbstractGraknGraph graknGraph, Vertex v){
        this.graknGraph = graknGraph;
        conceptId = ConceptId.of(v.id());
        vertex = v;
    }

    ConceptImpl(ConceptImpl concept){
        this.graknGraph = concept.getGraknGraph();
        this.conceptId = concept.getId();
        this.vertex = concept.getVertex();
    }

    /**
     *
     * @param key The key of the property to mutate
     * @param value The value to commit into the property
     * @return The concept itself casted to the correct interface itself
     */
    private T setProperty(String key, Object value){
        if(value == null) {
            getVertex().property(key).remove();
        } else {
            VertexProperty<Object> foundProperty = getVertex().property(key);
            if(foundProperty.isPresent() && foundProperty.value().equals(value)){
               return getThis();
            } else {
                getVertex().property(key, value);
            }
        }
        return getThis();
    }

    /**
     * Deletes the concept.
     * @throws ConceptException Throws an exception if the node has any edges attached to it.
     */
    @Override
    public void delete() throws ConceptException {
        deleteNode();
    }

    /**
     *
     * @param key The key of the unique property to mutate
     * @param id The new value of the unique property
     * @return The concept itself casted to the correct interface itself
     */
    T setUniqueProperty(Schema.ConceptProperty key, String id){
        if(graknGraph.isBatchLoadingEnabled() || updateAllowed(key, id)) {
            return setProperty(key, id);
        } else {
            throw new ConceptNotUniqueException(this, key, id);
        }
    }

    /**
     *
     * @param key The key of the unique property to mutate
     * @param value The value to check
     * @return True if the concept can be updated. I.e. the value is unique for the property.
     */
    private boolean updateAllowed(Schema.ConceptProperty key, String value) {
        Concept fetchedConcept = graknGraph.getConcept(key, value);
        return fetchedConcept == null || this.equals(fetchedConcept);
    }

    /**
     * Deletes the node and adds it neighbours for validation
     */
    void deleteNode(){
        graknGraph.getConceptLog().removeConcept(this);
        // delete node
        getVertex().remove();
    }

    /**
     * Helper method to cast a concept to it's correct type
     * @param type The type to cast to
     * @param <E> The type of the interface we are casting to.
     * @return The concept itself casted to the defined interface
     * @throws InvalidConceptTypeException when casting a concept incorrectly
     */
    private <E extends Concept> E castConcept(Class<E> type){
        try {
            return type.cast(this);
        } catch(ClassCastException e){
            throw new InvalidConceptTypeException(ErrorMessage.INVALID_OBJECT_TYPE.getMessage(this, type));
        }
    }

    /**
     *
     * @return A Type if the concept is a Type
     */
    @Override
    public Type asType() {
        return castConcept(Type.class);
    }

    /**
     *
     * @return An Instance if the concept is an Instance
     */
    @Override
    public Instance asInstance() {
        return castConcept(Instance.class);
    }

    /**
     *
     * @return A Entity Type if the concept is a Entity Type
     */
    @Override
    public EntityType asEntityType() {
        return castConcept(EntityType.class);
    }

    /**
     *
     * @return A Role Type if the concept is a Role Type
     */
    @Override
    public RoleType asRoleType() {
        return castConcept(RoleType.class);
    }

    /**
     *
     * @return A Relation Type if the concept is a Relation Type
     */
    @Override
    public RelationType asRelationType() {
        return castConcept(RelationType.class);
    }

    /**
     *
     * @return A Resource Type if the concept is a Resource Type
     */
    @SuppressWarnings("unchecked")
    @Override
    public <D> ResourceType<D> asResourceType() {
        return castConcept(ResourceType.class);
    }

    /**
     *
     * @return A Rule Type if the concept is a Rule Type
     */
    @Override
    public RuleType asRuleType() {
        return castConcept(RuleType.class);
    }

    /**
     *
     * @return An Entity if the concept is an Instance
     */
    @Override
    public Entity asEntity() {
        return castConcept(Entity.class);
    }

    /**
     *
     * @return A Relation if the concept is a Relation
     */
    @Override
    public Relation asRelation() {
        return castConcept(Relation.class);
    }

    /**
     *
     * @return A Resource if the concept is a Resource
     */
    @SuppressWarnings("unchecked")
    @Override
    public <D> Resource<D> asResource() {
        return castConcept(Resource.class);
    }

    /**
     *
     * @return A Rule if the concept is a Rule
     */@Override
    public Rule asRule() {
        return castConcept(Rule.class);
    }

    /**
     *
     * @return A casting if the concept is a casting
     */
    public CastingImpl asCasting(){
        return (CastingImpl) this;
    }

    /**
     *
     * @return true if the concept is a Type
     */
    @Override
    public boolean isType() {
        return this instanceof Type;
    }

    /**
     *
     * @return true if the concept is an Instance
     */
    @Override
    public boolean isInstance() {
        return this instanceof Instance;
    }

    /**
     *
     * @return true if the concept is a Entity Type
     */
    @Override
    public boolean isEntityType() {
        return this instanceof EntityType;
    }

    /**
     *
     * @return true if the concept is a Role Type
     */
    @Override
    public boolean isRoleType() {
        return this instanceof RoleType;
    }

    /**
     *
     * @return true if the concept is a Relation Type
     */
    @Override
    public boolean isRelationType() {
        return this instanceof RelationType;
    }

    /**
     *
     * @return true if the concept is a Resource Type
     */
    @Override
    public boolean isResourceType() {
        return this instanceof ResourceType;
    }

    /**
     *
     * @return true if the concept is a Rule Type
     */
    @Override
    public boolean isRuleType() {
        return this instanceof RuleType;
    }

    /**
     *
     * @return true if the concept is a Entity
     */
    @Override
    public boolean isEntity() {
        return this instanceof Entity;
    }

    /**
     *
     * @return true if the concept is a Relation
     */
    @Override
    public boolean isRelation() {
        return this instanceof Relation;
    }

    /**
     *
     * @return true if the concept is a Resource
     */
    @Override
    public boolean isResource() {
        return this instanceof Resource;
    }

    /**
     *
     * @return true if the concept is a Rule
     */
    @Override
    public boolean isRule() {
        return this instanceof Rule;
    }

    /**
     *
     * @return true if the concept is a casting
     */
    public boolean isCasting(){
        return this instanceof CastingImpl;
    }

    /**
     *
     * @param edgeType The edge label to traverse
     * @return The neighbouring concepts found by traversing outgoing edges of a specific type
     */
    <X extends Concept> Stream<X> getOutgoingNeighbours(Schema.EdgeLabel edgeType){
        return getEdgesOfType(Direction.OUT, edgeType).map(EdgeImpl::getTarget);
    }

    /**
     *
     * @param edgeType The edge label to traverse
     * @return The neighbouring concepts found by traversing incoming edges of a specific type
     */
    <X extends Concept> Stream<X> getIncomingNeighbours(Schema.EdgeLabel edgeType){
        return getEdgesOfType(Direction.IN, edgeType).map(EdgeImpl::getSource);
    }

    /**
     *
     * @param key The key of the non-unique property to mutate
     * @param value The value to commit into the property
     * @return The concept itself casted to the correct interface
     */
    T setProperty(Schema.ConceptProperty key, Object value){
        return setProperty(key.name(), value);
    }

    /**
     *
     * @param key The key of the non-unique property to retrieve
     * @return The value stored in the property
     */
    public <X> X getProperty(Schema.ConceptProperty key){
        VertexProperty<X> property = getVertex().property(key.name());
        if(property != null && property.isPresent()) {
            return property.value();
        }
        return null;
    }
    Boolean getPropertyBoolean(Schema.ConceptProperty key){
        Boolean value = getProperty(key);
        if(value == null) {
            return false;
        }
        return value;
    }

    /**
     *
     * @return The tinkerpop vertex
     */
    Vertex getVertex() {
        return vertex;
    }

    /**
     *
     * @return The base ttpe of this concept which helps us identify the concept
     */
    String getBaseType(){
        return getVertex().label();
    }

    /**
     *
     * @return A string representing the concept's unique id.
     */
    @Override
    public ConceptId getId(){
        return conceptId;
    }

    /**
     *
     * @param direction The direction of the edges to retrieve
     * @param type The type of the edges to retrieve
     * @return A collection of edges from this concept in a particular direction of a specific type
     */
    Stream<EdgeImpl> getEdgesOfType(Direction direction, Schema.EdgeLabel type){
        Iterable<Edge> iterable = () -> getVertex().edges(direction, type.getLabel());
        return StreamSupport.stream(iterable.spliterator(), false).
                map(edge -> getGraknGraph().getElementFactory().buildEdge(edge));
    }

    /**
     *
     * @return The grakn graph this concept is bound to.
     */
    protected AbstractGraknGraph<?> getGraknGraph() {return graknGraph;}

    //--------- Create Links -------//
    /**
     *  @param to the target concept
     * @param type the type of the edge to create
     */
    EdgeImpl putEdge(Concept to, Schema.EdgeLabel type){
        ConceptImpl toConcept = (ConceptImpl) to;
        GraphTraversal<Vertex, Edge> traversal = graknGraph.getTinkerPopGraph().traversal().V(getId().getRawValue()).outE(type.getLabel()).as("edge").otherV().hasId(toConcept.getId().getRawValue()).select("edge");
        if(!traversal.hasNext()) {
            return addEdge(toConcept, type);
        } else {
            return graknGraph.getElementFactory().buildEdge(traversal.next());
        }
    }

    /**
     *
     * @param toConcept the target concept
     * @param type the type of the edge to create
     * @return The edge created
     */
    EdgeImpl addEdge(ConceptImpl toConcept, Schema.EdgeLabel type) {
        return getGraknGraph().getElementFactory().buildEdge(toConcept.addEdgeFrom(getVertex(), type.getLabel()));
    }

    /**
     *
     * @param direction The direction of the edges to retrieve
     * @param type The type of the edges to retrieve
     */
    void deleteEdges(Direction direction, Schema.EdgeLabel type){
        getVertex().edges(direction, type.getLabel()).forEachRemaining(Element::remove);
    }

    /**
     * Deletes an edge of a specific type going to a specific concept
     * @param type The type of the edge
     * @param toConcept The target concept
     */
    void deleteEdgeTo(Schema.EdgeLabel type, Concept toConcept){
        GraphTraversal<Vertex, Edge> traversal = graknGraph.getTinkerPopGraph().traversal().V(getId().getRawValue()).
                outE(type.getLabel()).as("edge").otherV().hasId(toConcept.getId().getRawValue()).select("edge");
        if(traversal.hasNext()) {
            traversal.next().remove();
        }
    }

    private Edge addEdgeFrom(Vertex fromVertex, String type) {
        return fromVertex.addEdge(type, getVertex());
    }

    //------------ Base Equality ------------
    /**
     *
     * @return The hash code of the underlying vertex
     */
    public int hashCode() {
        return getId().hashCode(); //Note: This means that concepts across different transactions will be equivalent.
    }

    @Override
    public boolean equals(Object object) {
        //Compare Concept
        //based on id because vertex comparisons are equivalent
        if (this == object) return true;
        return object instanceof ConceptImpl && ((ConceptImpl) object).getId().equals(getId());
    }

    @Override
    public final String toString(){
        try {
            getGraknGraph().validVertex(vertex);
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
            throw new InvalidConceptValueException(ErrorMessage.NULL_VALUE.getMessage(conceptProperty.name()));
        }

        if(foundValue != null){
            if(!foundValue.equals(newValue)){
                throw new InvalidConceptValueException(ErrorMessage.IMMUTABLE_VALUE.getMessage(foundValue, this, newValue, conceptProperty.name()));
            }
        } else {
            setProperty(conceptProperty, converter.apply(newValue));
        }
    }
    
    @Override
    public int compareTo(Concept o) {
        return this.getId().compareTo(o.getId());
    }
}
