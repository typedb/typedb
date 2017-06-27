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
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.util.CommonUtil;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;

/**
 * <p>
 *     A Type represents any ontological element in the graph.
 * </p>
 *
 * <p>
 *     Types are used to model the behaviour of {@link Thing} and how they relate to each other.
 *     They also aid in categorising {@link Thing} to different types.
 * </p>
 *
 * @author fppt
 *
 * @param <T> The leaf interface of the object concept. For example an {@link ai.grakn.concept.EntityType} or {@link RelationType}
 * @param <V> The instance of this type. For example {@link ai.grakn.concept.Entity} or {@link ai.grakn.concept.Relation}
 */
class TypeImpl<T extends Type, V extends Thing> extends OntologyConceptImpl<T> implements Type{
    protected final Logger LOG = LoggerFactory.getLogger(TypeImpl.class);

    private Cache<Boolean> cachedIsAbstract = new Cache<>(() -> vertex().propertyBoolean(Schema.VertexProperty.IS_ABSTRACT));
    private Cache<Set<T>> cachedShards = new Cache<>(() -> this.<T>neighbours(Direction.IN, Schema.EdgeLabel.SHARD).collect(Collectors.toSet()));

    //This cache is different in order to keep track of which plays are required
    private Cache<Map<RoleType, Boolean>> cachedDirectPlays = new Cache<>(() -> {
        Map<RoleType, Boolean> roleTypes = new HashMap<>();

        vertex().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.PLAYS).forEach(edge -> {
            RoleType roleType = vertex().graph().factory().buildConcept(edge.target());
            Boolean required = edge.propertyBoolean(Schema.EdgeProperty.REQUIRED);
            roleTypes.put(roleType, required);
        });

        return roleTypes;
    });

    TypeImpl(VertexElement vertexElement) {
        super(vertexElement);
    }

    TypeImpl(VertexElement vertexElement, T superType) {
        super(vertexElement, superType);
    }

    TypeImpl(VertexElement vertexElement, T superType, Boolean isImplicit) {
        super(vertexElement, superType, isImplicit);
    }

    /**
     * Flushes the internal transaction caches so they can refresh with persisted graph
     */
    @Override
    public void txCacheFlush(){
        super.txCacheFlush();
        cachedIsAbstract.flush();
        cachedShards.flush();
        cachedDirectPlays.flush();
    }

    /**
     * Clears the internal transaction caches
     */
    @Override
    public void txCacheClear(){
        super.txCacheClear();
        cachedIsAbstract.clear();
        cachedShards.clear();
        cachedDirectPlays.clear();
    }

    /**
     * Utility method used to create or find an instance of this type
     *
     * @param instanceBaseType The base type of the instances of this type
     * @param finder The method to find the instrance if it already exists
     * @param producer The factory method to produce the instance if it doesn't exist
     * @return A new or already existing instance
     */
    V putInstance(Schema.BaseType instanceBaseType, Supplier<V> finder, BiFunction<VertexElement, T, V> producer) {
        vertex().graph().checkMutationAllowed();

        V instance = finder.get();
        if(instance == null) instance = addInstance(instanceBaseType, producer);
        return instance;
    }

    /**
     * Utility method used to create an instance of this type
     *
     * @param instanceBaseType The base type of the instances of this type
     * @param producer The factory method to produce the instance
     * @return A new instance
     */
    V addInstance(Schema.BaseType instanceBaseType, BiFunction<VertexElement, T, V> producer){
        vertex().graph().checkMutationAllowed();

        if(Schema.MetaSchema.isMetaLabel(getLabel()) && !Schema.MetaSchema.INFERENCE_RULE.getLabel().equals(getLabel()) && !Schema.MetaSchema.CONSTRAINT_RULE.getLabel().equals(getLabel())){
            throw GraphOperationException.metaTypeImmutable(getLabel());
        }

        if(isAbstract()) throw GraphOperationException.addingInstancesToAbstractType(this);

        VertexElement instanceVertex = vertex().graph().addVertex(instanceBaseType);
        if(!Schema.MetaSchema.isMetaLabel(getLabel())) {
            vertex().graph().txCache().addedInstance(getId());
        }
        return producer.apply(instanceVertex, getThis());
    }

    /**
     *
     * @return A list of all the roles this Type is allowed to play.
     */
    @Override
    public Collection<RoleType> plays() {
        Set<RoleType> allRoleTypes = new HashSet<>();

        //Get the immediate plays which may be cached
        allRoleTypes.addAll(cachedDirectPlays.get().keySet());

        //Now get the super type plays (Which may also be cached locally within their own context
        Set<T> superSet = superSet();
        superSet.remove(this); //We already have the plays from ourselves
        superSet.forEach(superParent -> allRoleTypes.addAll(((TypeImpl<?,?>) superParent).directPlays().keySet()));

        return Collections.unmodifiableCollection(filterImplicitStructures(allRoleTypes));
    }

    @Override
    public Collection<ResourceType> resources() {
        Collection<ResourceType> resources = resources(Schema.ImplicitType.HAS_OWNER);
        resources.addAll(keys());
        return resources;
    }

    @Override
    public Collection<ResourceType> keys() {
        return resources(Schema.ImplicitType.KEY_OWNER);
    }

    private Collection<ResourceType> resources(Schema.ImplicitType implicitType){
        return CommonUtil.withImplicitConceptsVisible(vertex().graph(), () -> {
            //TODO: Make this less convoluted
            String [] implicitIdentifiers = implicitType.getLabel("").getValue().split("--");
            String prefix = implicitIdentifiers[0] + "-";
            String suffix = "-" + implicitIdentifiers[1];

            vertex().graph().showImplicitConcepts(true); // If we don't set this to true no role types relating to resources will not be retrieved

            Set<ResourceType> resourceTypes = new HashSet<>();
            //A traversal is not used in this caching so that ontology caching can be taken advantage of.
            plays().forEach(roleType -> roleType.relationTypes().forEach(relationType -> {
                String roleTypeLabel = roleType.getLabel().getValue();
                if(roleTypeLabel.startsWith(prefix) && roleTypeLabel.endsWith(suffix)){ //This is the implicit type we want
                    String resourceTypeLabel = roleTypeLabel.replace(prefix, "").replace(suffix, "");
                    resourceTypes.add(vertex().graph().getResourceType(resourceTypeLabel));
                }
            }));

            return resourceTypes;
        });
    }

    Map<RoleType, Boolean> directPlays(){
        return cachedDirectPlays.get();
    }

    /**
     * Deletes the concept as type
     */
    @Override
    public void delete(){
        //If the deletion is successful we will need to update the cache of linked concepts. To do this caches must be loaded
        cachedDirectPlays.get();

        super.delete();

        //Updated caches of linked types
        cachedDirectPlays.get().keySet().forEach(roleType -> ((RoleTypeImpl) roleType).deleteCachedDirectPlaysByType(getThis()));
    }

    /**
     *
     * @return All the subs of this concept including itself
     */
    @Override
    public Collection<T> subTypes(){
        return Collections.unmodifiableCollection(filterImplicitStructures(super.subTypes()));
    }

    /**
     *
     * @return All the instances of this type.
     */
    @SuppressWarnings("unchecked")
    @Override
    public Collection<V> instances() {
        final Set<V> instances = new HashSet<>();

        GraphTraversal<Vertex, Vertex> traversal = vertex().graph().getTinkerPopGraph().traversal().V()
                .has(Schema.VertexProperty.TYPE_ID.name(), getTypeId().getValue())
                .union(__.identity(),
                        __.repeat(in(Schema.EdgeLabel.SUB.getLabel())).emit()
                ).unfold()
                .in(Schema.EdgeLabel.SHARD.getLabel())
                .in(Schema.EdgeLabel.ISA.getLabel());

        traversal.forEachRemaining(vertex -> {
            ConceptImpl concept = vertex().graph().factory().buildConcept(vertex);
            if (concept != null) instances.add((V) concept);
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
     * @return A list of the Instances which scope this Relation
     */
    @Override
    public Set<Thing> scopes() {
        HashSet<Thing> scopes = new HashSet<>();
        neighbours(Direction.OUT, Schema.EdgeLabel.HAS_SCOPE).forEach(concept -> scopes.add(concept.asInstance()));
        return scopes;
    }

    /**
     *
     * @param thing A new thing which can scope this concept
     * @return The concept itself
     */
    @Override
    public T scope(Thing thing) {
        putEdge(thing, Schema.EdgeLabel.HAS_SCOPE);
        return getThis();
    }

    /**
     * @param scope A concept which is currently scoping this concept.
     * @return The Relation itself
     */
    @Override
    public T deleteScope(Thing scope) {
        deleteEdge(Direction.OUT, Schema.EdgeLabel.HAS_SCOPE, (Concept) scope);
        return getThis();
    }

    void trackSuperChange(){
        instances().forEach(concept -> {
            if (concept.isInstance()) {
                ((ThingImpl<?, ?>) concept).castingsInstance().forEach(
                        rolePlayer -> vertex().graph().txCache().trackForValidation(rolePlayer));
            }
        });
    }

    T plays(RoleType roleType, boolean required) {
        checkOntologyMutationAllowed();

        //Update the internal cache of role types played
        cachedDirectPlays.ifPresent(map -> map.put(roleType, required));

        //Update the cache of types played by the role
        ((RoleTypeImpl) roleType).addCachedDirectPlaysByType(this);

        EdgeElement edge = putEdge(roleType, Schema.EdgeLabel.PLAYS);

        if (required) {
            edge.property(Schema.EdgeProperty.REQUIRED, true);
        }

        return getThis();
    }

    /**
     *
     * @param roleType The Role Type which the instances of this Type are allowed to play.
     * @return The Type itself.
     */
    public T plays(RoleType roleType) {
        return plays(roleType, false);
    }

    /**
     *
     * @param roleType The Role Type which the instances of this Type should no longer be allowed to play.
     * @return The Type itself.
     */
    @Override
    public T deletePlays(RoleType roleType) {
        checkOntologyMutationAllowed();
        deleteEdge(Direction.OUT, Schema.EdgeLabel.PLAYS, (Concept) roleType);
        cachedDirectPlays.ifPresent(set -> set.remove(roleType));
        ((RoleTypeImpl) roleType).deleteCachedDirectPlaysByType(this);

        //Add roleplayers to tracking to make sure they can still be played.
        instances().forEach(concept -> {
            if (concept.isInstance()) {
                ((ThingImpl<?, ?>) concept).castingsInstance().forEach(rolePlayer -> vertex().graph().txCache().trackForValidation(rolePlayer));
            }
        });


        return getThis();
    }

    /**
     *
     * @param isAbstract  Specifies if the concept is abstract (true) or not (false).
     *                    If the concept type is abstract it is not allowed to have any instances.
     * @return The Type itself.
     */
    public T setAbstract(Boolean isAbstract) {
        if(!Schema.MetaSchema.isMetaLabel(getLabel()) && isAbstract && currentShard().links().findAny().isPresent()){
            throw GraphOperationException.addingInstancesToAbstractType(this);
        }

        property(Schema.VertexProperty.IS_ABSTRACT, isAbstract);
        cachedIsAbstract.set(isAbstract);
        return getThis();
    }

    T property(Schema.VertexProperty key, Object value){
        if(!Schema.VertexProperty.CURRENT_TYPE_ID.equals(key)) checkOntologyMutationAllowed();
        vertex().property(key, value);
        return getThis();
    }

    /**
     * Creates a relation type which allows this type and a resource type to be linked.
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @param has the implicit relation type to build
     * @param hasValue the implicit role type to build for the resource type
     * @param hasOwner the implicit role type to build for the type
     * @param required Indicates if the resource is required on the entity
     * @return The Type itself
     */
    public T has(ResourceType resourceType, Schema.ImplicitType has, Schema.ImplicitType hasValue, Schema.ImplicitType hasOwner, boolean required){
        //Check if this is a met type
        checkOntologyMutationAllowed();

        //Check if resource type is the meta
        if(Schema.MetaSchema.RESOURCE.getLabel().equals(resourceType.getLabel())){
            throw GraphOperationException.metaTypeImmutable(resourceType.getLabel());
        }

        TypeLabel resourceTypeLabel = resourceType.getLabel();
        RoleType ownerRole = vertex().graph().putRoleTypeImplicit(hasOwner.getLabel(resourceTypeLabel));
        RoleType valueRole = vertex().graph().putRoleTypeImplicit(hasValue.getLabel(resourceTypeLabel));
        RelationType relationType = vertex().graph().putRelationTypeImplicit(has.getLabel(resourceTypeLabel)).
                relates(ownerRole).
                relates(valueRole);

        //Linking with ako structure if present
        ResourceType resourceTypeSuper = resourceType.superType();
        TypeLabel superLabel = resourceTypeSuper.getLabel();
        if(!Schema.MetaSchema.RESOURCE.getLabel().equals(superLabel)) { //Check to make sure we dont add plays edges to meta types accidentally
            RoleType ownerRoleSuper = vertex().graph().putRoleTypeImplicit(hasOwner.getLabel(superLabel));
            RoleType valueRoleSuper = vertex().graph().putRoleTypeImplicit(hasValue.getLabel(superLabel));
            RelationType relationTypeSuper = vertex().graph().putRelationTypeImplicit(has.getLabel(superLabel)).
                    relates(ownerRoleSuper).relates(valueRoleSuper);

            //Create the super type edges from sub role/relations to super roles/relation
            ownerRole.superType(ownerRoleSuper);
            valueRole.superType(valueRoleSuper);
            relationType.superType(relationTypeSuper);

            //Make sure the supertype resource is linked with the role as well
            ((ResourceTypeImpl) resourceTypeSuper).plays(valueRoleSuper);
        }

        this.plays(ownerRole, required);
        //TODO: Use explicit cardinality of 0-1 rather than just false
        ((ResourceTypeImpl) resourceType).plays(valueRole, false);

        return getThis();
    }

    /**
     * Creates a relation type which allows this type and a resource type to be linked.
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The Type itself
     */
    @Override
    public T resource(ResourceType resourceType){
        checkNonOverlapOfImplicitRelations(Schema.ImplicitType.KEY_OWNER, resourceType);
        return has(resourceType, Schema.ImplicitType.HAS, Schema.ImplicitType.HAS_VALUE, Schema.ImplicitType.HAS_OWNER, false);
    }

    @Override
    public T key(ResourceType resourceType) {
        checkNonOverlapOfImplicitRelations(Schema.ImplicitType.HAS_OWNER, resourceType);
        return has(resourceType, Schema.ImplicitType.KEY, Schema.ImplicitType.KEY_VALUE, Schema.ImplicitType.KEY_OWNER, true);
    }

    /**
     * Checks if the provided resource type is already used in an other implicit relation.
     *
     * @param implicitType The implicit relation to check against.
     * @param resourceType The resource type which should not be in that implicit relation
     *
     * @throws GraphOperationException when the resource type is already used in another implicit relation
     */
    private void checkNonOverlapOfImplicitRelations(Schema.ImplicitType implicitType, ResourceType resourceType){
        if(resources(implicitType).contains(resourceType)) {
            throw GraphOperationException.duplicateHas(this, resourceType);
        }
    }
}
