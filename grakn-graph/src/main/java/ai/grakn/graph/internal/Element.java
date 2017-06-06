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

/**
 * <p>
 *     Graph Element
 * </p>
 *
 * <p>
 *     Base class used to represent a construct in the graph. This includes exposed constructs such as {@link Concept}
 *     and hidden constructs such as {@link EdgeElement} and {@link RolePlayer}
 * </p>
 *
 * @author fppt
 *
 */
public abstract class Element {
    private final Object id;
    private final AbstractGraknGraph graknGraph;

    Element(AbstractGraknGraph graknGraph, Object id){
        this.graknGraph = graknGraph;
        this.id = id;
    }

    Object getElementId(){
        return id;
    }

    /**
     *
     * @return The grakn graph this concept is bound to.
     */
    protected AbstractGraknGraph<?> getGraknGraph() {
        return graknGraph;
    }

    /**
     *
     * @return The hash code of the underlying vertex
     */
    public int hashCode() {
        return id.hashCode(); //Note: This means that concepts across different transactions will be equivalent.
    }

    /**
     *
     * @return true if the elements equal each other
     */
    @Override
    public boolean equals(Object object) {
        //Compare Concept
        //based on id because vertex comparisons are equivalent
        return this == object || object instanceof Element && ((Element) object).getElementId().equals(getElementId());
    }

    /**
     * Helper method to cast a concept to it's correct type
     * @param type The type to cast to
     * @param <E> The type of the interface we are casting to.
     * @return The concept itself casted to the defined interface
     * @throws GraphOperationException when casting an element incorrectly
     */
    private <E extends Element> E castConcept(Class<E> type){
        try {
            return type.cast(this);
        } catch(ClassCastException e){
            throw GraphOperationException.invalidCasting(this, type);
        }
    }

    /**
     *
     * @return true if the element is a Concept
     */
    public ConceptImpl asConcept(){
        return castConcept(ConceptImpl.class);
    }

    /**
     *
     * @return A Type if the element is a Type
     */
    public Type asType() {
        return castConcept(TypeImpl.class);
    }

    /**
     *
     * @return An Instance if the element is an Instance
     */
    public Instance asInstance() {
        return castConcept(InstanceImpl.class);
    }

    /**
     *
     * @return A Entity Type if the element is a Entity Type
     */
    public EntityType asEntityType() {
        return castConcept(EntityTypeImpl.class);
    }

    /**
     *
     * @return A Role Type if the element is a Role Type
     */
    public RoleType asRoleType() {
        return castConcept(RoleTypeImpl.class);
    }

    /**
     *
     * @return A Relation Type if the element is a Relation Type
     */
    public RelationType asRelationType() {
        return castConcept(RelationTypeImpl.class);
    }

    /**
     *
     * @return A Resource Type if the element is a Resource Type
     */
    @SuppressWarnings("unchecked")
    public <D> ResourceType<D> asResourceType() {
        return castConcept(ResourceTypeImpl.class);
    }

    /**
     *
     * @return A Rule Type if the element is a Rule Type
     */
    public RuleType asRuleType() {
        return castConcept(RuleTypeImpl.class);
    }

    /**
     *
     * @return An Entity if the element is an Instance
     */
    public Entity asEntity() {
        return castConcept(EntityImpl.class);
    }

    /**
     *
     * @return A Relation if the element is a Relation
     */
    public Relation asRelation() {
        return castConcept(RelationImpl.class);
    }

    /**
     *
     * @return A Resource if the element is a Resource
     */
    @SuppressWarnings("unchecked")
    public <D> Resource<D> asResource() {
        return castConcept(ResourceImpl.class);
    }

    /**
     *
     * @return A Rule if the element is a Rule
     */
    public Rule asRule() {
        return castConcept(RuleImpl.class);
    }

    /**
     *
     * @return A casting if the element is a casting
     */
    public CastingImpl asCasting(){
        return castConcept(CastingImpl.class);
    }

    /**
     *
     * @return A roleplayer if the element is a roleplayer
     */
    public RolePlayer asRolePlayer(){
        return castConcept(RolePlayer.class);
    }

    /**
     *
     * @return true if the element is a Concept
     */
    public boolean isConcept(){
        return this instanceof Concept;
    }

    /**
     *
     * @return true if the element is a Type
     */
    public boolean isType() {
        return this instanceof Type;
    }

    /**
     *
     * @return true if the element is an Instance
     */
    public boolean isInstance() {
        return this instanceof Instance;
    }

    /**
     *
     * @return true if the element is a Entity Type
     */
    public boolean isEntityType() {
        return this instanceof EntityType;
    }

    /**
     *
     * @return true if the element is a Role Type
     */
    public boolean isRoleType() {
        return this instanceof RoleType;
    }

    /**
     *
     * @return true if the element is a Relation Type
     */
    public boolean isRelationType() {
        return this instanceof RelationType;
    }

    /**
     *
     * @return true if the element is a Resource Type
     */
    public boolean isResourceType() {
        return this instanceof ResourceType;
    }

    /**
     *
     * @return true if the element is a Rule Type
     */
    public boolean isRuleType() {
        return this instanceof RuleType;
    }

    /**
     *
     * @return true if the element is a Entity
     */
    public boolean isEntity() {
        return this instanceof Entity;
    }

    /**
     *
     * @return true if the element is a Relation
     */
    public boolean isRelation() {
        return this instanceof Relation;
    }

    /**
     *
     * @return true if the element is a Resource
     */
    public boolean isResource() {
        return this instanceof Resource;
    }

    /**
     *
     * @return true if the element is a Rule
     */
    public boolean isRule() {
        return this instanceof Rule;
    }

    /**
     *
     * @return true if the element is a casting
     */
    public boolean isCasting(){
        return this instanceof CastingImpl;
    }

    /**
     *
     * @return true if the element is a RolePlayer
     */
    public boolean isRolePlayer(){
        return this instanceof RolePlayer;
    }
}
