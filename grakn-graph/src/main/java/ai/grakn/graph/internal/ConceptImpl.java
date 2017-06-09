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
import ai.grakn.exception.GraphOperationException;
import ai.grakn.exception.PropertyNotUniqueException;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;
import java.util.function.Function;
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
 * @param <T> The leaf interface of the object concept.
 *           For example an {@link EntityType}, {@link Entity}, {@link RelationType} etc . . .
 */
abstract class ConceptImpl<T extends Concept> implements Concept {
    private final ElementCache<Boolean> cachedIsShard = new ElementCache<>(() -> getPropertyBoolean(Schema.ConceptProperty.IS_SHARD));
    private final VertexElement vertexElement;

    @SuppressWarnings("unchecked")
    T getThis(){
        return (T) this;
    }

    ConceptImpl(VertexElement vertexElement){
        this.vertexElement = vertexElement;
    }

    public VertexElement getVertexElement() {
        return vertexElement;
    }

    AbstractGraknGraph<?> graph(){
        return getVertexElement().getGraknGraph();
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
        if(!getVertexElement().getGraknGraph().isBatchGraph()) {
            Concept fetchedConcept = getVertexElement().getGraknGraph().getConcept(key, value);
            if (fetchedConcept != null) throw PropertyNotUniqueException.cannotChangeProperty(this, fetchedConcept, key, value);
        }

        return setProperty(key, value);
    }

    /**
     * Deletes the node and adds it neighbours for validation
     */
    void deleteNode(){
        //TODO: clean this
        graph().txCache().remove(this);
        // delete node
        getVertexElement().getElement().remove();
    }

    /**
     *
     * @param edgeType The edge label to traverse
     * @return The neighbouring concepts found by traversing outgoing edges of a specific type
     */
    <X extends Concept> Stream<X> getOutgoingNeighbours(Schema.EdgeLabel edgeType){
        return getVertexElement().getEdgesOfType(Direction.OUT, edgeType).map(EdgeElement::getTarget);
    }

    /**
     *
     * @param edgeType The edge label to traverse
     * @return The neighbouring concepts found by traversing incoming edges of a specific type
     */
    <X extends Concept> Stream<X> getIncomingNeighbours(Schema.EdgeLabel edgeType){
        return getVertexElement().getEdgesOfType(Direction.IN, edgeType).map(EdgeElement::getSource);
    }


    EdgeElement putEdge(Concept to, Schema.EdgeLabel label){
        return getVertexElement().putEdge(((ConceptImpl) to).getVertexElement(), label);
    }

    EdgeElement addEdge(Concept to, Schema.EdgeLabel label){
        return getVertexElement().addEdge(((ConceptImpl) to).getVertexElement(), label);
    }

    void deleteEdge(Direction direction, Schema.EdgeLabel label, Concept... to) {
        if (to.length == 0) {
            getVertexElement().deleteEdge(direction, label);
        } else{
            VertexElement[] targets = new VertexElement[to.length];
            for (int i = 0; i < to.length; i++) {
                targets[i] = ((ConceptImpl)to[i]).getVertexElement();
            }
            getVertexElement().deleteEdge(direction, label, targets);
        }
    }

    /**
     *
     * @param key The key of the non-unique property to mutate
     * @param value The value to commit into the property
     * @return The concept itself casted to the correct interface
     */
    T setProperty(Schema.ConceptProperty key, Object value){
        getVertexElement().setProperty(key.name(), value);
        return getThis();
    }

    /**
     *
     * @param key The key of the non-unique property to retrieve
     * @return The value stored in the property
     */
    public <X> X getProperty(Schema.ConceptProperty key){
        return getVertexElement().getProperty(key.name());
    }
    Boolean getPropertyBoolean(Schema.ConceptProperty key){
        return getVertexElement().getPropertyBoolean(key.name());
    }

    /**
     *
     * @return The base type of this concept which helps us identify the concept
     */
    Schema.BaseType getBaseType(){
        return Schema.BaseType.valueOf(getVertexElement().label());
    }

    /**
     *
     * @return A string representing the concept's unique id.
     */
    @Override
    public ConceptId getId(){
        return ConceptId.of(getVertexElement().getElementId().getValue());
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
            graph().validVertex(getVertexElement().getElement());
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
        Vertex shardVertex = getVertexElement().getGraknGraph().addVertex(getBaseType());
        shardVertex.addEdge(Schema.EdgeLabel.SHARD.getLabel(), getVertexElement().getElement());

        ConceptImpl shardConcept = graph().buildConcept(shardVertex);
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
        return getVertexElement().getGraknGraph().getConcept(ConceptId.of(currentShardId));
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

    /**
     * Helper method to cast a concept to it's correct type
     * @param type The type to cast to
     * @param <E> The type of the interface we are casting to.
     * @return The concept itself casted to the defined interface
     * @throws GraphOperationException when casting an element incorrectly
     */
    private <E extends Concept> E castConcept(Class<E> type){
        try {
            return type.cast(this);
        } catch(ClassCastException e){
            throw GraphOperationException.invalidCasting(this, type);
        }
    }

    /**
     *
     * @return A Type if the element is a Type
     */
    @Override
    public Type asType() {
        return castConcept(Type.class);
    }

    /**
     *
     * @return An Instance if the element is an Instance
     */
    @Override
    public Instance asInstance() {
        return castConcept(Instance.class);
    }

    /**
     *
     * @return A Entity Type if the element is a Entity Type
     */
    @Override
    public EntityType asEntityType() {
        return castConcept(EntityType.class);
    }

    /**
     *
     * @return A Role Type if the element is a Role Type
     */
    @Override
    public RoleType asRoleType() {
        return castConcept(RoleType.class);
    }

    /**
     *
     * @return A Relation Type if the element is a Relation Type
     */
    @Override
    public RelationType asRelationType() {
        return castConcept(RelationType.class);
    }

    /**
     *
     * @return A Resource Type if the element is a Resource Type
     */
    @SuppressWarnings("unchecked")
    @Override
    public <D> ResourceType<D> asResourceType() {
        return castConcept(ResourceType.class);
    }

    /**
     *
     * @return A Rule Type if the element is a Rule Type
     */
    @Override
    public RuleType asRuleType() {
        return castConcept(RuleType.class);
    }

    /**
     *
     * @return An Entity if the element is an Instance
     */
    @Override
    public Entity asEntity() {
        return castConcept(Entity.class);
    }

    /**
     *
     * @return A Relation if the element is a Relation
     */
    @Override
    public Relation asRelation() {
        return castConcept(Relation.class);
    }

    /**
     *
     * @return A Resource if the element is a Resource
     */
    @SuppressWarnings("unchecked")
    @Override
    public <D> Resource<D> asResource() {
        return castConcept(Resource.class);
    }

    /**
     *
     * @return A Rule if the element is a Rule
     */
    @Override
    public Rule asRule() {
        return castConcept(Rule.class);
    }

    /**
     *
     * @return true if the element is a Type
     */
    @Override
    public boolean isType() {
        return this instanceof Type;
    }

    /**
     *
     * @return true if the element is an Instance
     */
    @Override
    public boolean isInstance() {
        return this instanceof Instance;
    }

    /**
     *
     * @return true if the element is a Entity Type
     */
    @Override
    public boolean isEntityType() {
        return this instanceof EntityType;
    }

    /**
     *
     * @return true if the element is a Role Type
     */
    @Override
    public boolean isRoleType() {
        return this instanceof RoleType;
    }

    /**
     *
     * @return true if the element is a Relation Type
     */
    @Override
    public boolean isRelationType() {
        return this instanceof RelationType;
    }

    /**
     *
     * @return true if the element is a Resource Type
     */
    @Override
    public boolean isResourceType() {
        return this instanceof ResourceType;
    }

    /**
     *
     * @return true if the element is a Rule Type
     */
    @Override
    public boolean isRuleType() {
        return this instanceof RuleType;
    }

    /**
     *
     * @return true if the element is a Entity
     */
    @Override
    public boolean isEntity() {
        return this instanceof Entity;
    }

    /**
     *
     * @return true if the element is a Relation
     */
    @Override
    public boolean isRelation() {
        return this instanceof Relation;
    }

    /**
     *
     * @return true if the element is a Resource
     */
    @Override
    public boolean isResource() {
        return this instanceof Resource;
    }

    /**
     *
     * @return true if the element is a Rule
     */
    @Override
    public boolean isRule() {
        return this instanceof Rule;
    }
}
