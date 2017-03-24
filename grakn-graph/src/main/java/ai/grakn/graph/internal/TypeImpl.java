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
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
class TypeImpl<T extends Type, V extends Instance> extends ConceptImpl<T> implements Type{
    protected final Logger LOG = LoggerFactory.getLogger(TypeImpl.class);

    private TypeName cachedTypeName;
    private ComponentCache<Boolean> cachedIsImplicit = new ComponentCache<>(() -> getPropertyBoolean(Schema.ConceptProperty.IS_IMPLICIT));
    private ComponentCache<Boolean> cachedIsAbstract = new ComponentCache<>(() -> getPropertyBoolean(Schema.ConceptProperty.IS_ABSTRACT));
    private ComponentCache<T> cachedSuperType = new ComponentCache<>(() -> this.<T>getOutgoingNeighbours(Schema.EdgeLabel.SUB).findFirst().orElse(null));
    private ComponentCache<Set<T>> cachedDirectSubTypes = new ComponentCache<>(() -> this.<T>getIncomingNeighbours(Schema.EdgeLabel.SUB).collect(Collectors.toSet()));

    //This cache is different in order to keep track of which plays roles are required
    private ComponentCache<Map<RoleType, Boolean>> cachedDirectPlaysRoles = new ComponentCache<>(() -> {
        Map<RoleType, Boolean> roleTypes = new HashMap<>();

        getEdgesOfType(Direction.OUT, Schema.EdgeLabel.PLAYS_ROLE).forEach(edge -> {
            RoleType roleType = edge.getTarget();
            Boolean required = edge.getPropertyBoolean(Schema.EdgeProperty.REQUIRED);
            roleTypes.put(roleType, required);
        });

        return roleTypes;
    });

    TypeImpl(AbstractGraknGraph graknGraph, Vertex v) {
        super(graknGraph, v);
        cachedTypeName = TypeName.of(v.value(Schema.ConceptProperty.NAME.name()));
    }

    TypeImpl(AbstractGraknGraph graknGraph, Vertex v, T superType) {
        this(graknGraph, v);
        if(superType() == null) superType(superType);
    }

    TypeImpl(AbstractGraknGraph graknGraph, Vertex v, T superType, Boolean isImplicit) {
        this(graknGraph, v, superType);
        setImmutableProperty(Schema.ConceptProperty.IS_IMPLICIT, isImplicit, getProperty(Schema.ConceptProperty.IS_IMPLICIT), Function.identity());
        cachedIsImplicit.set(isImplicit);
    }

    TypeImpl(TypeImpl<T, V> type) {
        super(type);
        this.cachedTypeName = type.getName();
        type.cachedIsImplicit.ifPresent(value -> this.cachedIsImplicit.set(value));
        type.cachedIsAbstract.ifPresent(value -> this.cachedIsAbstract.set(value));
    }

    @Override
    public Type copy(){
        //noinspection unchecked
        return new TypeImpl(this);
    }

    @SuppressWarnings("unchecked")
    void copyCachedConcepts(T type){
        ((TypeImpl<T, V>) type).cachedSuperType.ifPresent(value -> this.cachedSuperType.set(getGraknGraph().clone(value)));
        ((TypeImpl<T, V>) type).cachedDirectSubTypes.ifPresent(value -> this.cachedDirectSubTypes.set(getGraknGraph().clone(value)));
    }

    /**
     *
     * @param instanceBaseType The base type of the instances of this type
     * @param producer The factory method to produce the instance
     * @return The instance required
     */
    V addInstance(Schema.BaseType instanceBaseType, BiFunction<Vertex, T, V> producer){
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
        allRoleTypes.addAll(cachedDirectPlaysRoles.get().keySet());

        //Now get the super type plays roles (Which may also be cached locally within their own context
        Set<T> superSet = superTypeSet();
        superSet.remove(this); //We already have the plays roles from ourselves
        superSet.forEach(superParent -> allRoleTypes.addAll(((TypeImpl<?,?>) superParent).directPlaysRoles().keySet()));

        return Collections.unmodifiableCollection(filterImplicitStructures(allRoleTypes));
    }

    @Override
    public Collection<ResourceType> resources() {
        boolean implicitFlag = getGraknGraph().implicitConceptsVisible();
        
        getGraknGraph().showImplicitConcepts(true); // If we don't set this to true no role types relating to resources will not be retreived

        Set<ResourceType> resourceTypes = new HashSet<>();
        //A traversal is not used in this caching so that ontology caching can be taken advantage of.
        playsRoles().forEach(roleType -> roleType.relationTypes().forEach(relationType -> {
            if(relationType.isImplicit()){
                //This is faster than doing the traversal
                TypeName prefix = Schema.Resource.HAS_RESOURCE.getName(TypeName.of(""));
                TypeName resourceTypeName = TypeName.of(relationType.getName().getValue().replace(prefix.getValue(), ""));
                resourceTypes.add(getGraknGraph().getType(resourceTypeName));
            }
        }));

        getGraknGraph().showImplicitConcepts(implicitFlag);
        return resourceTypes;
    }

    Map<RoleType, Boolean> directPlaysRoles(){
        return cachedDirectPlaysRoles.get();
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
    public void delete(){
        checkTypeMutation();
        boolean hasSubs = getVertex().edges(Direction.IN, Schema.EdgeLabel.SUB.getLabel()).hasNext();
        boolean hasInstances = getVertex().edges(Direction.IN, Schema.EdgeLabel.ISA.getLabel()).hasNext();

        if(hasSubs || hasInstances){
            throw new ConceptException(ErrorMessage.CANNOT_DELETE.getMessage(getName()));
        } else {
            //Force load of linked concepts whose caches need to be updated
            cachedSuperType.get();
            cachedDirectPlaysRoles.get();

            deleteNode();

            //Update neighbouring caches
            //noinspection unchecked
            ((TypeImpl<T, V>) cachedSuperType.get()).deleteCachedDirectedSubType(getThis());
            cachedDirectPlaysRoles.get().keySet().forEach(roleType -> ((RoleTypeImpl) roleType).deleteCachedDirectPlaysByType(getThis()));

            //Clear internal caching
            cachedIsImplicit.clear();
            cachedIsAbstract.clear();
            cachedSuperType.clear();
            cachedDirectSubTypes.clear();
            cachedDirectPlaysRoles.clear();

            //Clear Global ComponentCache
            getGraknGraph().getConceptLog().removeConcept(this);
        }
    }

    /**
     *
     * @return This type's super type
     */
    public T superType() {
        return cachedSuperType.get();
    }

    /**
     *
     * @return All outgoing sub parents including itself
     */
    Set<T> superTypeSet() {
        Set<T> superSet= new HashSet<>();
        superSet.add(getThis());
        T superParent = superType();

        while(superParent != null && !Schema.MetaSchema.CONCEPT.getName().equals(superParent.getName())){
            superSet.add(superParent);
            //noinspection unchecked
            superParent = (T) superParent.superType();
        }

        return superSet;
    }

    /**
     *
     * @param root The current type to example
     * @return All the sub children of the root. Effectively calls  the cache {@link TypeImpl#cachedDirectSubTypes} recursively
     */
    @SuppressWarnings("unchecked")
    private Set<T> nextSubLevel(TypeImpl<T, V> root){
        Set<T> results = new HashSet<>();
        results.add((T) root);

        Set<T> children = root.cachedDirectSubTypes.get();
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
        return Collections.unmodifiableCollection(filterImplicitStructures(nextSubLevel(this)));
    }

    /**
     * Adds a new sub type to the currently cached sub types. If no subtypes have been cached then this will hit the database.
     *
     * @param newSubType The new subtype
     */
    private void addCachedDirectSubType(T newSubType){
        cachedDirectSubTypes.ifPresent(set -> set.add(newSubType));
    }

    /**
     * Removes an old sub type from the currently cached sub types. If no subtypes have been cached then this will hit the database.
     *
     * @param oldSubType The old sub type which should not be cached anymore
     */
    private void deleteCachedDirectedSubType(T oldSubType){
        cachedDirectSubTypes.ifPresent(set -> set.remove(oldSubType));
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

        return Collections.unmodifiableCollection(filterImplicitStructures(instances));
    }

    /**
     *
     * @return returns true if the type is set to be abstract.
     */
    @Override
    public Boolean isAbstract() {
        return cachedIsAbstract.get();
    }

    /**
     *
     * @return returns true if the type was created implicitly through {@link #hasResource}
     */
    @Override
    public Boolean isImplicit(){
        return cachedIsImplicit.get();
    }

    /**
     *
     * @return A collection of Rules for which this Type serves as a hypothesis
     */
    @Override
    public Collection<Rule> getRulesOfHypothesis() {
        Set<Rule> rules = new HashSet<>();
        getIncomingNeighbours(Schema.EdgeLabel.HYPOTHESIS).forEach(concept -> rules.add(concept.asRule()));
        return Collections.unmodifiableCollection(rules);
    }

    /**
     *
     * @return A collection of Rules for which this Type serves as a conclusion
     */
    @Override
    public Collection<Rule> getRulesOfConclusion() {
        Set<Rule> rules = new HashSet<>();
        getIncomingNeighbours(Schema.EdgeLabel.CONCLUSION).forEach(concept -> rules.add(concept.asRule()));
        return Collections.unmodifiableCollection(rules);
    }

    /**
     *
     * @return A list of the Instances which scope this Relation
     */
    @Override
    public Set<Instance> scopes() {
        HashSet<Instance> scopes = new HashSet<>();
        getOutgoingNeighbours(Schema.EdgeLabel.HAS_SCOPE).forEach(concept -> scopes.add(concept.asInstance()));
        return scopes;
    }

    /**
     *
     * @param instance A new instance which can scope this concept
     * @return The concept itself
     */
    @Override
    public T scope(Instance instance) {
        putEdge(instance, Schema.EdgeLabel.HAS_SCOPE);
        return getThis();
    }

    /**
     * @param scope A concept which is currently scoping this concept.
     * @return The Relation itself
     */
    @Override
    public T deleteScope(Instance scope) {
        deleteEdgeTo(Schema.EdgeLabel.HAS_SCOPE, scope);
        return getThis();
    }

    /**
     *
     * @param newSuperType This type's super type
     * @return The Type itself
     */
    public T superType(T newSuperType) {
        checkTypeMutation();

        T oldSuperType = superType();
        if(oldSuperType == null || (!oldSuperType.equals(newSuperType))) {
            //Update the super type of this type in cache
            cachedSuperType.set(newSuperType);

            //Note the check before the actual construction
            if(superTypeLoops()){
                cachedSuperType.set(oldSuperType); //Reset if the new super type causes a loop
                throw new ConceptException(ErrorMessage.SUPER_TYPE_LOOP_DETECTED.getMessage(getName(), newSuperType.getName()));
            }

            //Modify the graph once we have checked no loop occurs
            deleteEdges(Direction.OUT, Schema.EdgeLabel.SUB);
            putEdge(newSuperType, Schema.EdgeLabel.SUB);

            //Update the sub types of the old super type
            if(oldSuperType != null) {
                //noinspection unchecked - Casting is needed to access {deleteCachedDirectedSubTypes} method
                ((TypeImpl<T, V>) oldSuperType).deleteCachedDirectedSubType(getThis());
            }

            //Add this as the subtype to the supertype
            //noinspection unchecked - Casting is needed to access {addCachedDirectSubTypes} method
            ((TypeImpl<T, V>) newSuperType).addCachedDirectSubType(getThis());

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

        //Update the internal cache of role types played
        cachedDirectPlaysRoles.ifPresent(map -> map.put(roleType, required));

        //Update the cache of types played by the role
        ((RoleTypeImpl) roleType).addCachedDirectPlaysByType(this);

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
        cachedDirectPlaysRoles.ifPresent(set -> set.remove(roleType));
        ((RoleTypeImpl) roleType).deleteCachedDirectPlaysByType(this);

        //Add castings to tracking to make sure they can still be played.
        instances().forEach(concept -> {
            if (concept.isInstance()) {
                ((InstanceImpl<?, ?>) concept).castings().forEach(casting -> getGraknGraph().getConceptLog().trackConceptForValidation(casting));
            }
        });

        return getThis();
    }

    @Override
    public String innerToString(){
        String message = super.innerToString();
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
        if(isAbstract) getGraknGraph().getConceptLog().trackConceptForValidation(this);
        cachedIsAbstract.set(isAbstract);
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
    public T hasResource(ResourceType resourceType, boolean required){
        //Check if this is a met type
        checkTypeMutation();

        //Check if resource type is the meta
        if(Schema.MetaSchema.RESOURCE.getName().equals(resourceType.getName())){
            throw new ConceptException(ErrorMessage.META_TYPE_IMMUTABLE.getMessage(resourceType.getName()));
        }

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

            //Create the super type edges from sub role/relations to super roles/relation
            ownerRole.superType(ownerRoleSuper);
            valueRole.superType(valueRoleSuper);
            relationType.superType(relationTypeSuper);

            //Make sure the supertype resource is linked with the role as well
            ((ResourceTypeImpl) resourceTypeSuper).playsRole(valueRoleSuper);
        }

        this.playsRole(ownerRole, required);
        ((ResourceTypeImpl) resourceType).playsRole(valueRole, required);

        return getThis();
    }

    /**
     *
     * @return The name of this type
     */
    @Override
    public TypeName getName() {
        return cachedTypeName;
    }

    /**
     * Creates a relation type which allows this type and a resource type to be linked.
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The resulting relation type which allows instances of this type to have relations with the provided resourceType.
     */
    @Override
    public T hasResource(ResourceType resourceType){
        return hasResource(resourceType, false);
    }

    @Override
    public T key(ResourceType resourceType) {
        return hasResource(resourceType, true);
    }
}
