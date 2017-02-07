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
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;
import ai.grakn.exception.ConceptException;
import ai.grakn.exception.ConceptNotUniqueException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * <p>
 *     A Type represents any ontological element in the graph.
 * </p>
 *
 * <p>
 *     Types are used to model the behaviour of {@link Instance} and how they relate to each other.
 *     They also aid in categorising {@link Instance} to different types.
 * </p>
 *
 * @author fppt
 *
 * @param <T> The leaf interface of the object concept. For example an {@link ai.grakn.concept.EntityType} or {@link RelationType}
 * @param <V> The instance of this type. For example {@link ai.grakn.concept.Entity} or {@link ai.grakn.concept.Relation}
 */
class TypeImpl<T extends Type, V extends Instance> extends ConceptImpl<T> implements Type {
    private TypeName cachedTypeName;
    private Optional<T> cachedSuperType = Optional.empty();
    private Optional<Set<T>> cachedDirectSubTypes = Optional.empty();
    private Optional<Set<RoleType>> cachedPlaysRoles = Optional.empty(); //Optional is used so we know if we have to read from the DB or not.

    TypeImpl(AbstractGraknGraph graknGraph, Vertex v) {
        super(graknGraph, v);
        getName();//This is called to ensure the cachedTypeName is loaded.
    }

    TypeImpl(AbstractGraknGraph graknGraph, Vertex v, T superType) {
        this(graknGraph, v);
        if(superType() == null) superType(superType);
    }

    TypeImpl(AbstractGraknGraph graknGraph, Vertex v, T superType, Boolean isImplicit) {
        this(graknGraph, v, superType);
        setImmutableProperty(Schema.ConceptProperty.IS_IMPLICIT, isImplicit, getProperty(Schema.ConceptProperty.IS_IMPLICIT), Function.identity());
    }

    /**
     *
     * @param instanceBaseType The base type of the instances of this type
     * @param producer The factory method to produce the instance
     * @return The instance required
     */
    protected V addInstance(Schema.BaseType instanceBaseType, BiFunction<Vertex, T, V> producer){
        if(Schema.MetaSchema.isMetaName(getName()) && !Schema.MetaSchema.INFERENCE_RULE.getName().equals(getName()) && !Schema.MetaSchema.CONSTRAINT_RULE.getName().equals(getName())){
            throw new ConceptException(ErrorMessage.META_TYPE_IMMUTABLE.getMessage(getName()));
        }

        Vertex instanceVertex = getGraknGraph().addVertex(instanceBaseType);
        return producer.apply(instanceVertex, getThis());
    }

    /**
     *
     * @return A list of all the roles this Type is allowed to play.
     */
    @Override
    public Collection<RoleType> playsRoles() {
        Set<RoleType> allRoleTypes = new HashSet<>();

        //Get the immediate plays roles which may be cached
        if(!cachedPlaysRoles.isPresent()) {
            cachedPlaysRoles = Optional.of(getOutgoingNeighbours(Schema.EdgeLabel.PLAYS_ROLE));
        }
        allRoleTypes.addAll(cachedPlaysRoles.get());

        //Now get the super type plays roles (Which may also be cached locally within their own context
        Set<T> superSet = getSuperSet();
        superSet.remove(this); //We already have the plays roles from ourselves
        superSet.forEach(superParent -> allRoleTypes.addAll(superParent.playsRoles()));

        return filterImplicitStructures(allRoleTypes);
    }

    private <X extends Concept> Set<X> filterImplicitStructures(Set<X> types){
        if (!getGraknGraph().implicitConceptsVisible() && !types.isEmpty() && types.iterator().next().isType()) {
            return types.stream().filter(t -> !t.asType().isImplicit()).collect(Collectors.toSet());
        }
        return types;
    }

    /**
     * Deletes the concept as  type
     */
    @Override
    public void innerDelete(){
        checkTypeMutation();
        boolean hasSubs = getVertex().edges(Direction.IN, Schema.EdgeLabel.SUB.getLabel()).hasNext();
        boolean hasInstances = getVertex().edges(Direction.IN, Schema.EdgeLabel.ISA.getLabel()).hasNext();

        if(hasSubs || hasInstances){
            throw new ConceptException(ErrorMessage.CANNOT_DELETE.getMessage(getName()));
        } else {
            deleteNode();
        }
    }

    /**
     *
     * @return This type's super type
     */
    public T superType() {
        if(!cachedSuperType.isPresent()){
            T concept = getOutgoingNeighbour(Schema.EdgeLabel.SUB);
            if(concept == null) {
                return null;
            }
            cachedSuperType = Optional.of(concept);
        }
        return cachedSuperType.get();
    }

    /**
     * Changes the name of the type
     *
     * @param name The new name of the type
     * @return The Type name
     */
    public TypeName setName(String name){
        //TODO: Propagate name change to all instances
        TypeName typeName = TypeName.of(name);
        Type foundType = getGraknGraph().getType(typeName);

        if(foundType == null) {
            setProperty(Schema.ConceptProperty.NAME, name);
        } else if (!equals(foundType)){
            throw new ConceptNotUniqueException(foundType, name);
        }
        cachedTypeName = typeName;
        return cachedTypeName;
    }

    /**
     *
     * @return All outgoing sub parents including itself
     */
    Set<T> getSuperSet() {
        Set<T> superSet= new HashSet<>();
        superSet.add(getThis());
        T superParent = superType();

        while(superParent != null && !Schema.MetaSchema.CONCEPT.getName().equals(superParent.getName())){
            if(superSet.contains(superParent)) {
                throw new ConceptException(ErrorMessage.LOOP_DETECTED.getMessage(toString(), Schema.EdgeLabel.SUB.getLabel()));
            } else {
                superSet.add(superParent);
            }
            //noinspection unchecked
            superParent = (T) superParent.superType();
        }

        return superSet;
    }

    /**
     *
     * @param root The current type to example
     * @return All the sub children of the root. Effectively calls  {@link TypeImpl#directSubTypes()} recursively
     */
    @SuppressWarnings("unchecked")
    private Set<T> nextSubLevel(TypeImpl<T, V> root){
        Set<T> results = new HashSet<>();
        results.add((T) root);

        Set<T> children = root.directSubTypes();
        for(T child: children){
            results.addAll(nextSubLevel((TypeImpl<T, V>) child));
        }

        return results;
    }

    /**
     *
     * @return All the subtypes of this concept including itself
     */
    @Override
    public Collection<T> subTypes(){
        return filterImplicitStructures(nextSubLevel(this));
    }

    /**
     *
     * @return All of the concepts direct sub children spanning a single level.
     */
    private Set<T> directSubTypes(){
        if(!cachedDirectSubTypes.isPresent()){
            cachedDirectSubTypes = Optional.of(getIncomingNeighbours(Schema.EdgeLabel.SUB));
        }
        return cachedDirectSubTypes.get();
    }

    /**
     * Updates the currently cached sub type. If no subtypes have been cached then this will hit the database.
     *
     * @param newSubType The new subtype
     */
    private void addCachedDirectSubTypes(T newSubType){
        directSubTypes();//Called to make sure the current children have been cached
        cachedDirectSubTypes.map(set -> set.add(newSubType));
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
                .has(Schema.ConceptProperty.NAME.name(), getName().getValue())
                .union(__.identity(), __.repeat(__.in(Schema.EdgeLabel.SUB.getLabel())).emit()).unfold()
                .in(Schema.EdgeLabel.ISA.getLabel());

        traversal.forEachRemaining(vertex -> {
            ConceptImpl<Concept> concept = getGraknGraph().getElementFactory().buildConcept(vertex);
            if(!concept.isCasting()){
                instances.add((V) concept);
            }
        });

        return filterImplicitStructures(instances);
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
     * @param superType This type's super type
     * @return The Type itself
     */
    public T superType(T superType) {
        checkTypeMutation();

        T currentSuperType = superType();
        if(currentSuperType == null || (!currentSuperType.equals(superType))) {
            //Update the super type of this type in cache
            cachedSuperType = Optional.of(superType);

            //Note the check before the actual construction
            if(superTypeLoops()){
                cachedSuperType = Optional.ofNullable(currentSuperType); //Reset if the new super type causes a loop
                throw new ConceptException(ErrorMessage.LOOP_DETECTED.getMessage(getName(), Schema.EdgeLabel.SUB.getLabel()));
            }

            //Modify the graph once we have checked no loop occurs
            deleteEdges(Direction.OUT, Schema.EdgeLabel.SUB);
            putEdge(superType, Schema.EdgeLabel.SUB);

            //Add this as the subtype to the supertype
            //noinspection unchecked - Casting is needed to access {updateCachedImmediateSubTypes} method
            ((TypeImpl<T, V>) superType).addCachedDirectSubTypes(getThis());

            //Track any existing data if there is some
            instances().forEach(concept -> {
                if (concept.isInstance()) {
                    ((InstanceImpl<?, ?>) concept).castings().forEach(
                            instance -> getGraknGraph().getConceptLog().trackConceptForValidation(instance));
                }
            });
        }
        
        return getThis();
    }

    /**
     * Adds another subtype to this type
     *
     * @param type The sub type of this type
     * @return The Type itself
     */
    public T subType(T type){
        //noinspection unchecked
        ((TypeImpl) type).superType(this);
        return getThis();
    }

    private boolean superTypeLoops(){
        //Check For Loop
        HashSet<Type> foundTypes = new HashSet<>();
        Type currentSuperType = superType();
        while (currentSuperType != null){
            foundTypes.add(currentSuperType);
            currentSuperType = currentSuperType.superType();
            if(foundTypes.contains(currentSuperType)){
                return true;
            }
        }
        return false;
    }

    T playsRole(RoleType roleType, boolean required) {
        checkTypeMutation();
        EdgeImpl edge = putEdge(roleType, Schema.EdgeLabel.PLAYS_ROLE);

        if(!cachedPlaysRoles.isPresent()){
            cachedPlaysRoles = Optional.of(new HashSet<>());
        }
        cachedPlaysRoles.get().add(roleType);

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
        cachedPlaysRoles.map(set -> set.remove(roleType));

        //Add castings to tracking to make sure they can still be played.
        instances().forEach(concept -> {
            if (concept.isInstance()) {
                ((InstanceImpl<?, ?>) concept).castings().forEach(casting -> getGraknGraph().getConceptLog().trackConceptForValidation(casting));
            }
        });

        return getThis();
    }

    @Override
    public String toString(){
        String message = super.toString();
        message = message + " - Name [" + getName() + "] - Abstract [" + isAbstract() + "] ";
        return message;
    }

    /**
     *
     * @param isAbstract  Specifies if the concept is abstract (true) or not (false).
     *                    If the concept type is abstract it is not allowed to have any instances.
     * @return The Type itself.
     */
    public T setAbstract(Boolean isAbstract) {
        setProperty(Schema.ConceptProperty.IS_ABSTRACT, isAbstract);
        if(isAbstract) {
            getGraknGraph().getConceptLog().trackConceptForValidation(this);
        }
        return getThis();
    }

    @Override
    T setProperty(Schema.ConceptProperty key, Object value){
        checkTypeMutation();
        return super.setProperty(key, value);
    }

    /**
     * Checks if we are mutating a type in a valid way. Type mutations are valid if:
     * 1. The type is not a meta-type
     * 2. The graph is not batch loading
     */
    void checkTypeMutation(){
        getGraknGraph().checkOntologyMutation();
        if(Schema.MetaSchema.isMetaName(getName())){
            throw new ConceptException(ErrorMessage.META_TYPE_IMMUTABLE.getMessage(getName()));
        }
    }

    /**
     * Creates a relation type which allows this type and a resource type to be linked.
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @param required Indicates if the resource is required on the entity
     * @return The resulting relation type which allows instances of this type to have relations with the provided resourceType.
     */
    public RelationType hasResource(ResourceType resourceType, boolean required){
        TypeName resourceTypeName = resourceType.getName();
        RoleType ownerRole = getGraknGraph().putRoleTypeImplicit(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceTypeName));
        RoleType valueRole = getGraknGraph().putRoleTypeImplicit(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceTypeName));
        RelationType relationType = getGraknGraph().putRelationTypeImplicit(Schema.Resource.HAS_RESOURCE.getName(resourceTypeName)).
                hasRole(ownerRole).
                hasRole(valueRole);

        //Linking with ako structure if present
        ResourceType resourceTypeSuper = resourceType.superType();
        TypeName superName = resourceTypeSuper.getName();
        if(!Schema.MetaSchema.RESOURCE.getName().equals(superName)) { //Check to make sure we dont add plays role edges to meta types accidentally
            RoleType ownerRoleSuper = getGraknGraph().putRoleTypeImplicit(Schema.Resource.HAS_RESOURCE_OWNER.getName(superName));
            RoleType valueRoleSuper = getGraknGraph().putRoleTypeImplicit(Schema.Resource.HAS_RESOURCE_VALUE.getName(superName));
            RelationType relationTypeSuper = getGraknGraph().putRelationTypeImplicit(Schema.Resource.HAS_RESOURCE.getName(superName)).
                    hasRole(ownerRoleSuper).hasRole(valueRoleSuper);

            if(ownerRole.equals(ownerRoleSuper)){
                System.out.println("WHAT?????");
            }

            //Create the super type edges from sub role/relations to super roles/relation
            ownerRole.superType(ownerRoleSuper);
            valueRole.superType(valueRoleSuper);
            relationType.superType(relationTypeSuper);

            //Make sure the supertype resource is linked with the role as well
            ((ResourceTypeImpl) resourceTypeSuper).playsRole(valueRoleSuper);
        }

        this.playsRole(ownerRole, required);
        ((ResourceTypeImpl) resourceType).playsRole(valueRole, required);

        return relationType;
    }

    /**
     *
     * @return The name of this type
     */
    @Override
    public TypeName getName() {
        if(cachedTypeName == null){
            cachedTypeName = TypeName.of(getProperty(Schema.ConceptProperty.NAME));
        }
        return cachedTypeName;
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
