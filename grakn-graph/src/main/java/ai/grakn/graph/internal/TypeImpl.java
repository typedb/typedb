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
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import ai.grakn.exception.ConceptException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A Type represents any ontological element in the graph. For example Entity Types and Rule Types.
 * @param <T> The leaf interface of the object concept. For example an EntityType, Entity, RelationType etc . . .
 * @param <V> The type of the instances of this concept type.
 */
class TypeImpl<T extends Type, V extends Concept> extends ConceptImpl<T, Type> implements Type {
    TypeImpl(Vertex v, Type type, Boolean isImplicit, AbstractGraknGraph graknGraph) {
        super(v, type, graknGraph);
        setImmutableProperty(Schema.ConceptProperty.IS_IMPLICIT, isImplicit, getProperty(Schema.ConceptProperty.IS_IMPLICIT), Function.identity());
    }
    TypeImpl(Vertex v, Type type, AbstractGraknGraph graknGraph) {
        super(v, type, graknGraph);
    }

    /**
     *
     * @param instanceBaseType The base type of the instances of this type
     * @param producer The factory method to produce the instance
     * @return The instance required
     */
    protected V addInstance(Schema.BaseType instanceBaseType, BiFunction<Vertex, T, V> producer){
        Vertex instanceVertex = getGraknGraph().addVertex(instanceBaseType);
        return producer.apply(instanceVertex, getThis());
    }

    /**
     *
     * @return A list of all the roles this Type is allowed to play.
     */
    @Override
    public Collection<RoleType> playsRoles() {
        return getOutgoingNeighbours(Schema.EdgeLabel.PLAYS_ROLE);
    }

    /**
     * Deletes the concept as a type
     */
    @Override
    public void innerDelete(){
        Collection<? extends Concept> subSet = subTypes();
        Collection<? extends Concept> instanceSet = instances();
        subSet.remove(this);

        if(subSet.isEmpty() && instanceSet.isEmpty()){
            deleteNode();
        } else {
            throw new ConceptException(ErrorMessage.CANNOT_DELETE.getMessage(toString()));
        }
    }

    /**
     *
     * @return All outgoing sub parents including itself
     */
    Set<T> getSubHierarchySuperSet() {
        Set<T> superSet= new HashSet<>();
        superSet.add(getThis());
        T subParent = superType();

        while(subParent != null){
            if(superSet.contains(subParent))
                throw new ConceptException(ErrorMessage.LOOP_DETECTED.getMessage(toString(), Schema.EdgeLabel.SUB.getLabel()));
            else
                superSet.add(subParent);
            //noinspection unchecked
            subParent = (T) subParent.superType();
        }

        return superSet;
    }

    /**
     *
     * @param root The current type to example
     * @return All the sub children of the root. Effectively calls  {@link TypeImpl#getSubConceptTypes()} recursively
     */
    @SuppressWarnings("unchecked")
    private Set<T> nextSubLevel(TypeImpl<?, ?> root){
        Set<T> results = new HashSet<>();
        results.add((T) root);

        Collection<TypeImpl<Type, Concept>> children = root.getSubConceptTypes();
        for(TypeImpl<Type, Concept> child: children){
            results.addAll(nextSubLevel(child));
        }

        return results;
    }

    /**
     *
     * @return All the subtypes of this concept including itself
     */
    @Override
    public Collection<T> subTypes(){
        return nextSubLevel(this);
    }

    /**
     *
     * @return All of the concepts direct sub children spanning a single level.
     */
    private Collection<TypeImpl<Type, Concept>> getSubConceptTypes(){
        Collection<TypeImpl<Type, Concept>> subSet = new HashSet<>();
        getIncomingNeighbours(Schema.EdgeLabel.SUB).forEach(concept -> subSet.add((TypeImpl<Type, Concept>) concept));
        return subSet;
    }

    /**
     *
     * @return All the instances of this type.
     */
    @SuppressWarnings("unchecked")
    @Override
    public Collection<V> instances() {
        Set<V> instances = new HashSet<>();

        //noinspection unchecked
        GraphTraversal<Vertex, Vertex> traversal = getGraknGraph().getTinkerPopGraph().traversal().V()
                .has(Schema.ConceptProperty.NAME.name(), getName())
                .union(__.identity(), __.repeat(__.in(Schema.EdgeLabel.SUB.getLabel())).emit()).unfold()
                .in(Schema.EdgeLabel.ISA.getLabel())
                .union(__.identity(), __.repeat(__.in(Schema.EdgeLabel.SUB.getLabel())).emit()).unfold();

        traversal.forEachRemaining(vertex -> {
            ConceptImpl<Concept, Type> concept = getGraknGraph().getElementFactory().buildUnknownConcept(vertex);
            if(!Schema.BaseType.CASTING.name().equals(concept.getBaseType())){
                instances.add((V) concept);
            }
        });

        return instances;
    }

    /**
     *
     * @return returns true if the type is set to be abstract.
     */
    @Override
    public Boolean isAbstract() {
        return getPropertyBoolean(Schema.ConceptProperty.IS_ABSTRACT);
    }

    /**
     *
     * @return returns true if the type was created implicitly through {@link #hasResource}
     */
    @Override
    public Boolean isImplicit(){
        return getPropertyBoolean(Schema.ConceptProperty.IS_IMPLICIT);
    }

    /**
     *
     * @return A collection of Rules for which this Type serves as a hypothesis
     */
    @Override
    public Collection<Rule> getRulesOfHypothesis() {
        Set<Rule> rules = new HashSet<>();
        getIncomingNeighbours(Schema.EdgeLabel.HYPOTHESIS).forEach(concept -> rules.add(concept.asRule()));
        return rules;
    }

    /**
     *
     * @return A collection of Rules for which this Type serves as a conclusion
     */
    @Override
    public Collection<Rule> getRulesOfConclusion() {
        Set<Rule> rules = new HashSet<>();
        getIncomingNeighbours(Schema.EdgeLabel.CONCLUSION).forEach(concept -> rules.add(concept.asRule()));
        return rules;
    }

    /**
     *
     * @param type This type's super type
     * @return The Type itself
     */
    public T superType(T type) {
        ((TypeImpl) type).checkTypeMutation();
        checkTypeMutation();

        //Track any existing data if there is some
        Type currentSuperType = superType();
        if(currentSuperType != null){
            currentSuperType.instances().forEach(concept -> {
                if(concept.isInstance()){
                    ((InstanceImpl<?, ?>) concept).castings().forEach(
                            instance -> getGraknGraph().getConceptLog().putConcept(instance));
                }
            });
        }

        deleteEdges(Direction.OUT, Schema.EdgeLabel.SUB);
        deleteEdges(Direction.OUT, Schema.EdgeLabel.ISA);
        putEdge(type, Schema.EdgeLabel.SUB);
        type(); //Check if there is a circular sub loop
        return getThis();
    }

    T playsRole(RoleType roleType, boolean required) {
        checkTypeMutation();
        EdgeImpl edge = putEdge(roleType, Schema.EdgeLabel.PLAYS_ROLE);

        if (required) {
            edge.setProperty(Schema.EdgeProperty.REQUIRED, true);
        }

        return getThis();
    }

    /**
     *
     * @param roleType The Role Type which the instances of this Type are allowed to play.
     * @return The Type itself.
     */
    public T playsRole(RoleType roleType) {
        return playsRole(roleType, false);
    }

    /**
     *
     * @param roleType The Role Type which the instances of this Type should no longer be allowed to play.
     * @return The Type itself.
     */
    @Override
    public T deletePlaysRole(RoleType roleType) {
        checkTypeMutation();
        deleteEdgeTo(Schema.EdgeLabel.PLAYS_ROLE, roleType);

        //Add castings to tracking to make sure they can still be played.
        instances().forEach(concept -> {
            if (concept.isInstance()) {
                ((InstanceImpl<?, ?>) concept).castings().forEach(casting -> getGraknGraph().getConceptLog().putConcept(casting));
            }
        });

        return getThis();
    }

    @Override
    public String toString(){
        String message = super.toString();
        message = message + " - Abstract [" + isAbstract() + "] ";
        return message;
    }

    /**
     *
     * @param isAbstract  Specifies if the concept is abstract (true) or not (false).
     *                    If the concept type is abstract it is not allowed to have any instances.
     * @return The Type itself.
     */
    public T setAbstract(Boolean isAbstract) {
        checkTypeMutation();
        setProperty(Schema.ConceptProperty.IS_ABSTRACT, isAbstract);
        if(isAbstract)
            getGraknGraph().getConceptLog().putConcept(this);
        return getThis();
    }

    /**
     * Checks if we are mutating a type in a valid way. Type mutations are valid if:
     * 1. The type is not a meta-type
     * 2. The graph is not batch loading
     */
    protected void checkTypeMutation(){
        getGraknGraph().checkOntologyMutation();
        for (Schema.MetaSchema metaSchema : Schema.MetaSchema.values()) {
            if(metaSchema.getId().equals(getName())){
                throw new ConceptException(ErrorMessage.META_TYPE_IMMUTABLE.getMessage(metaSchema.getId()));
            }
        }
    }

    /**
     * Creates a relation type which allows this type and a resource type to be linked.
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The resulting relation type which allows instances of this type to have relations with the provided resourceType.
     */
    @Override
    public RelationType hasResource(ResourceType resourceType, boolean required){
        String resourceTypeId = resourceType.getName();
        RoleType ownerRole = getGraknGraph().putRoleTypeImplicit(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceTypeId));
        RoleType valueRole = getGraknGraph().putRoleTypeImplicit(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceTypeId));

        this.playsRole(ownerRole, required);
        ((ResourceTypeImpl) resourceType).playsRole(valueRole, required);

        return getGraknGraph().
                putRelationTypeImplicit(Schema.Resource.HAS_RESOURCE.getId(resourceTypeId)).
                hasRole(ownerRole).
                hasRole(valueRole);
    }

    /**
     *
     * @return The name of this type
     */
    @Override
    public String getName() {
        return getProperty(Schema.ConceptProperty.NAME);
    }

    /**
     * Creates a relation type which allows this type and a resource type to be linked.
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The resulting relation type which allows instances of this type to have relations with the provided resourceType.
     */
    @Override
    public RelationType hasResource(ResourceType resourceType){
        return hasResource(resourceType, false);
    }

    @Override
    public RelationType key(ResourceType resourceType) {
        return hasResource(resourceType, true);
    }
}
