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

package ai.grakn.graph.internal.concept;

import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.graph.internal.cache.Cache;
import ai.grakn.graph.internal.cache.Cacheable;
import ai.grakn.graph.internal.structure.EdgeElement;
import ai.grakn.graph.internal.structure.Shard;
import ai.grakn.graph.internal.structure.VertexElement;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
public class TypeImpl<T extends Type, V extends Thing> extends SchemaConceptImpl<T> implements Type{
    protected final Logger LOG = LoggerFactory.getLogger(TypeImpl.class);

    private final Cache<Boolean> cachedIsAbstract = new Cache<>(Cacheable.bool(), () -> vertex().propertyBoolean(Schema.VertexProperty.IS_ABSTRACT));
    private final Cache<Set<T>> cachedShards = new Cache<>(Cacheable.set(), () -> this.<T>neighbours(Direction.IN, Schema.EdgeLabel.SHARD).collect(Collectors.toSet()));

    //This cache is different in order to keep track of which plays are required
    private final Cache<Map<Role, Boolean>> cachedDirectPlays = new Cache<>(Cacheable.map(), () -> {
        Map<Role, Boolean> roleTypes = new HashMap<>();

        vertex().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.PLAYS).forEach(edge -> {
            Role role = vertex().graph().factory().buildConcept(edge.target());
            Boolean required = edge.propertyBoolean(Schema.EdgeProperty.REQUIRED);
            roleTypes.put(role, required);
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
        preCheckForInstanceCreation();

        V instance = finder.get();
        if(instance == null) instance = addInstance(instanceBaseType, producer, false);
        return instance;
    }

    /**
     * Utility method used to create an instance of this type
     *
     * @param instanceBaseType The base type of the instances of this type
     * @param producer The factory method to produce the instance
     * @param checkNeeded indicates if a check is necessary before adding the instance
     * @return A new instance
     */
    V addInstance(Schema.BaseType instanceBaseType, BiFunction<VertexElement, T, V> producer, boolean checkNeeded){
        if(checkNeeded) preCheckForInstanceCreation();

        if(isAbstract()) throw GraphOperationException.addingInstancesToAbstractType(this);

        VertexElement instanceVertex = vertex().graph().addVertex(instanceBaseType);
        if(!Schema.MetaSchema.isMetaLabel(getLabel())) {
            vertex().graph().txCache().addedInstance(getId());
        }
        return producer.apply(instanceVertex, getThis());
    }

    /**
     * Checks if an {@link Thing} is allowed to be created and linked to this {@link Type}.
     * This can fail is the {@link ai.grakn.GraknTxType} is read only.
     * It can also fail when attempting to attach a resource to a meta type
     */
    private void preCheckForInstanceCreation(){
        vertex().graph().checkMutationAllowed();

        if(Schema.MetaSchema.isMetaLabel(getLabel()) && !Schema.MetaSchema.INFERENCE_RULE.getLabel().equals(getLabel()) && !Schema.MetaSchema.CONSTRAINT_RULE.getLabel().equals(getLabel())){
            throw GraphOperationException.metaTypeImmutable(getLabel());
        }
    }

    /**
     *
     * @return A list of all the roles this Type is allowed to play.
     */
    @Override
    public Stream<Role> plays() {
        //Get the immediate plays which may be cached
        Stream<Role> allRoles = directPlays().keySet().stream();

        //Now get the super type plays (Which may also be cached locally within their own context
        Stream<Role> superSet =superSet().
                filter(sup -> !sup.equals(this)). //We already have the plays from ourselves
                flatMap(sup -> TypeImpl.from(sup).directPlays().keySet().stream());

        return Stream.concat(allRoles, superSet);
    }

    @Override
    public Stream<ResourceType> resources() {
        Stream<ResourceType> resources = resources(Schema.ImplicitType.HAS_OWNER);
        return Stream.concat(resources, keys());
    }

    @Override
    public Stream<ResourceType> keys() {
        return resources(Schema.ImplicitType.KEY_OWNER);
    }

    private Stream<ResourceType> resources(Schema.ImplicitType implicitType){
        //TODO: Make this less convoluted
        String [] implicitIdentifiers = implicitType.getLabel("").getValue().split("--");
        String prefix = implicitIdentifiers[0] + "-";
        String suffix = "-" + implicitIdentifiers[1];

        //A traversal is not used in this so that ontology caching can be taken advantage of.
        return plays().map(role -> role.getLabel().getValue()).
                filter(roleLabel -> roleLabel.startsWith(prefix) && roleLabel.endsWith(suffix)).
                map(roleLabel -> {
                    String resourceTypeLabel = roleLabel.replace(prefix, "").replace(suffix, "");
                    return vertex().graph().getResourceType(resourceTypeLabel);
                });
    }

    public Map<Role, Boolean> directPlays(){
        return cachedDirectPlays.get();
    }

    /**
     * Deletes the concept as type
     */
    @Override
    public void delete(){

        //If the deletion is successful we will need to update the cache of linked concepts. To do this caches must be loaded
        Map<Role, Boolean> plays = cachedDirectPlays.get();

        super.delete();

        //Updated caches of linked types
        plays.keySet().forEach(roleType -> ((RoleImpl) roleType).deleteCachedDirectPlaysByType(getThis()));
    }
    @Override
    boolean deletionAllowed(){
        return super.deletionAllowed() && !currentShard().links().findAny().isPresent();
    }

    /**
     *
     * @return All the instances of this type.
     */
    @SuppressWarnings("unchecked")
    @Override
    public Stream<V> instances() {
        return subs().flatMap(sub -> TypeImpl.<T, V>from(sub).instancesDirect());
    }

    Stream<V> instancesDirect(){
        return vertex().getEdgesOfType(Direction.IN, Schema.EdgeLabel.SHARD).
                map(edge -> vertex().graph().factory().buildShard(edge.source())).
                flatMap(Shard::<V>links);
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
    public Stream<Thing> scopes() {
        return neighbours(Direction.OUT, Schema.EdgeLabel.HAS_SCOPE);
    }

    /**
     *
     * @param thing A new thing which can scope this concept
     * @return The concept itself
     */
    @Override
    public T scope(Thing thing) {
        putEdge(ConceptVertex.from(thing), Schema.EdgeLabel.HAS_SCOPE);
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

    void trackRolePlayers(){
        instances().forEach(concept -> ((ThingImpl<?, ?>)concept).castingsInstance().forEach(
                rolePlayer -> vertex().graph().txCache().trackForValidation(rolePlayer)));
    }

    public T plays(Role role, boolean required) {
        checkOntologyMutationAllowed();

        //Update the internal cache of role types played
        cachedDirectPlays.ifPresent(map -> map.put(role, required));

        //Update the cache of types played by the role
        ((RoleImpl) role).addCachedDirectPlaysByType(this);

        EdgeElement edge = putEdge(ConceptVertex.from(role), Schema.EdgeLabel.PLAYS);

        if (required) {
            edge.property(Schema.EdgeProperty.REQUIRED, true);
        }

        return getThis();
    }

    /**
     *
     * @param role The Role Type which the instances of this Type are allowed to play.
     * @return The Type itself.
     */
    public T plays(Role role) {
        return plays(role, false);
    }

    /**
     * This is a temporary patch to prevent accidentally disconnecting implicit {@link RelationType}s from their
     * {@link RelationEdge}s. This Disconnection happens because {@link RelationType#instances()} depends on the
     * presence of a direct {@link Schema.EdgeLabel#PLAYS} edge between the {@link Type} and the implicit {@link RelationType}.
     *
     * When changing the super you may accidentally cause this disconnection. So we prevent it here.
     *
     */
    //TODO: Remove this when traversing to the instances of an implicit Relation Type is no longer done via plays edges
    @Override
    boolean changingSuperAllowed(T oldSuperType, T newSuperType){
        boolean changingSuperAllowed = super.changingSuperAllowed(oldSuperType, newSuperType);
        if(changingSuperAllowed && oldSuperType != null && !Schema.MetaSchema.isMetaLabel(oldSuperType.getLabel())) {
            //noinspection unchecked
            Set<Role> superPlays = oldSuperType.plays().collect(Collectors.toSet());

            //Get everything that this can play bot including the supers
            Set<Role> plays = new HashSet<>(directPlays().keySet());
            subs().flatMap(sub -> TypeImpl.from(sub).directPlays().keySet().stream()).
                    forEach(play -> plays.add((Role) play));

            superPlays.removeAll(plays);

            //It is possible to be disconnecting from a role which is no longer in use but checking this will take too long
            //So we assume the role is in sure and throw if that is the case
            if(!superPlays.isEmpty() && instancesDirect().findAny().isPresent()){
                throw GraphOperationException.changingSuperWillDisconnectRole(oldSuperType, newSuperType, superPlays.iterator().next());
            }

            return true;
        }
        return changingSuperAllowed;
    }

    /**
     *
     * @param role The Role Type which the instances of this Type should no longer be allowed to play.
     * @return The Type itself.
     */
    @Override
    public T deletePlays(Role role) {
        checkOntologyMutationAllowed();
        deleteEdge(Direction.OUT, Schema.EdgeLabel.PLAYS, (Concept) role);
        cachedDirectPlays.ifPresent(set -> set.remove(role));
        ((RoleImpl) role).deleteCachedDirectPlaysByType(this);

        trackRolePlayers();

        return getThis();
    }

    /**
     *
     * @param isAbstract  Specifies if the concept is abstract (true) or not (false).
     *                    If the concept type is abstract it is not allowed to have any instances.
     * @return The Type itself.
     */
    public T setAbstract(Boolean isAbstract) {
        if(!Schema.MetaSchema.isMetaLabel(getLabel()) && isAbstract && instancesDirect().findAny().isPresent()){
            throw GraphOperationException.addingInstancesToAbstractType(this);
        }

        property(Schema.VertexProperty.IS_ABSTRACT, isAbstract);
        cachedIsAbstract.set(isAbstract);
        return getThis();
    }

    public T property(Schema.VertexProperty key, Object value){
        if(!Schema.VertexProperty.CURRENT_LABEL_ID.equals(key)) checkOntologyMutationAllowed();
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
    private T has(ResourceType resourceType, Schema.ImplicitType has, Schema.ImplicitType hasValue, Schema.ImplicitType hasOwner, boolean required){
        //Check if this is a met type
        checkOntologyMutationAllowed();

        //Check if resource type is the meta
        if(Schema.MetaSchema.RESOURCE.getLabel().equals(resourceType.getLabel())){
            throw GraphOperationException.metaTypeImmutable(resourceType.getLabel());
        }

        Label resourceLabel = resourceType.getLabel();
        Role ownerRole = vertex().graph().putRoleTypeImplicit(hasOwner.getLabel(resourceLabel));
        Role valueRole = vertex().graph().putRoleTypeImplicit(hasValue.getLabel(resourceLabel));
        RelationType relationType = vertex().graph().putRelationTypeImplicit(has.getLabel(resourceLabel)).
                relates(ownerRole).
                relates(valueRole);

        //Linking with ako structure if present
        ResourceType resourceTypeSuper = resourceType.sup();
        Label superLabel = resourceTypeSuper.getLabel();
        if(!Schema.MetaSchema.RESOURCE.getLabel().equals(superLabel)) { //Check to make sure we dont add plays edges to meta types accidentally
            Role ownerRoleSuper = vertex().graph().putRoleTypeImplicit(hasOwner.getLabel(superLabel));
            Role valueRoleSuper = vertex().graph().putRoleTypeImplicit(hasValue.getLabel(superLabel));
            RelationType relationTypeSuper = vertex().graph().putRelationTypeImplicit(has.getLabel(superLabel)).
                    relates(ownerRoleSuper).relates(valueRoleSuper);

            //Create the super type edges from sub role/relations to super roles/relation
            ownerRole.sup(ownerRoleSuper);
            valueRole.sup(valueRoleSuper);
            relationType.sup(relationTypeSuper);

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
        if(resources(implicitType).anyMatch(rt -> rt.equals(resourceType))) {
            throw GraphOperationException.duplicateHas(this, resourceType);
        }
    }

    public static <X extends Type, Y extends Thing> TypeImpl<X,Y> from(Type type){
        //noinspection unchecked
        return (TypeImpl<X, Y>) type;
    }
}
