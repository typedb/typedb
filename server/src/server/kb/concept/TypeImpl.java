/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.server.kb.concept;

import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.Entity;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.Relation;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Thing;
import grakn.core.graql.concept.Type;
import grakn.core.graql.internal.Schema;
import grakn.core.server.Transaction;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.kb.cache.Cache;
import grakn.core.server.kb.cache.Cacheable;
import grakn.core.server.kb.structure.EdgeElement;
import grakn.core.server.kb.structure.Shard;
import grakn.core.server.kb.structure.VertexElement;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A Type represents any ontological element in the graph.
 * Types are used to model the behaviour of {@link Thing} and how they relate to each other.
 * They also aid in categorising {@link Thing} to different types.
 *
 * @param <T> The leaf interface of the object concept. For example an {@link EntityType} or {@link RelationType}
 * @param <V> The instance of this type. For example {@link Entity} or {@link Relation}
 */
public class TypeImpl<T extends Type, V extends Thing> extends SchemaConceptImpl<T> implements Type {

    private final Cache<Boolean> cachedIsAbstract = Cache.createSessionCache(this, Cacheable.bool(), () -> vertex().propertyBoolean(Schema.VertexProperty.IS_ABSTRACT));

    //This cache is different in order to keep track of which plays are required
    private final Cache<Map<Role, Boolean>> cachedDirectPlays = Cache.createSessionCache(this, Cacheable.map(), () -> {
        Map<Role, Boolean> roleTypes = new HashMap<>();

        vertex().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.PLAYS).forEach(edge -> {
            Role role = vertex().tx().factory().buildConcept(edge.target());
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
        //This constructor is ONLY used when CREATING new types. Which is why we shard here
        createShard();
    }

    /**
     * Utility method used to create an instance of this type
     *
     * @param instanceBaseType The base type of the instances of this type
     * @param producer         The factory method to produce the instance
     * @return A new instance
     */
    V addInstance(Schema.BaseType instanceBaseType, BiFunction<VertexElement, T, V> producer, boolean isInferred) {
        preCheckForInstanceCreation();

        if (isAbstract()) throw TransactionException.addingInstancesToAbstractType(this);

        VertexElement instanceVertex = vertex().tx().addVertexElement(instanceBaseType);
        if (!Schema.MetaSchema.isMetaLabel(label())) {
            vertex().tx().cache().addedInstance(id());
            if (isInferred) instanceVertex.property(Schema.VertexProperty.IS_INFERRED, true);
        }
        V instance = producer.apply(instanceVertex, getThis());
        assert instance != null : "producer should never return null";
        return instance;
    }

    /**
     * Checks if an {@link Thing} is allowed to be created and linked to this {@link Type}.
     * This can fail is the {@link Transaction.Type} is read only.
     * It can also fail when attempting to attach an {@link Attribute} to a meta type
     */
    private void preCheckForInstanceCreation() {
        vertex().tx().checkMutationAllowed();

        if (Schema.MetaSchema.isMetaLabel(label())) {
            throw TransactionException.metaTypeImmutable(label());
        }
    }

    /**
     * @return A list of all the roles this Type is allowed to play.
     */
    @Override
    public Stream<Role> playing() {
        //Get the immediate plays which may be cached
        Stream<Role> allRoles = directPlays().keySet().stream();

        //Now get the super type plays (Which may also be cached locally within their own context
        Stream<Role> superSet = this.sups().
                filter(sup -> !sup.equals(this)). //We already have the plays from ourselves
                flatMap(sup -> TypeImpl.from(sup).directPlays().keySet().stream());

        return Stream.concat(allRoles, superSet);
    }

    @Override
    public Stream<AttributeType> attributes() {
        Stream<AttributeType> attributes = attributes(Schema.ImplicitType.HAS_OWNER);
        return Stream.concat(attributes, keys());
    }

    @Override
    public Stream<AttributeType> keys() {
        return attributes(Schema.ImplicitType.KEY_OWNER);
    }

    private Stream<AttributeType> attributes(Schema.ImplicitType implicitType) {
        //TODO: Make this less convoluted
        String[] implicitIdentifiers = implicitType.getLabel("").getValue().split("--");
        String prefix = implicitIdentifiers[0] + "-";
        String suffix = "-" + implicitIdentifiers[1];

        //A traversal is not used in this so that caching can be taken advantage of.
        return playing().map(role -> role.label().getValue()).
                filter(roleLabel -> roleLabel.startsWith(prefix) && roleLabel.endsWith(suffix)).
                map(roleLabel -> {
                    String attributeTypeLabel = roleLabel.replace(prefix, "").replace(suffix, "");
                    return vertex().tx().getAttributeType(attributeTypeLabel);
                });
    }

    public Map<Role, Boolean> directPlays() {
        return cachedDirectPlays.get();
    }

    /**
     * Deletes the concept as type
     */
    @Override
    public void delete() {

        //If the deletion is successful we will need to update the cache of linked concepts. To do this caches must be loaded
        Map<Role, Boolean> plays = cachedDirectPlays.get();

        super.delete();

        //Updated caches of linked types
        plays.keySet().forEach(roleType -> ((RoleImpl) roleType).deleteCachedDirectPlaysByType(getThis()));
    }

    @Override
    boolean deletionAllowed() {
        return super.deletionAllowed() && !currentShard().links().findAny().isPresent();
    }

    /**
     * @return All the instances of this type.
     */
    @SuppressWarnings("unchecked")
    @Override
    public Stream<V> instances() {
        return subs().flatMap(sub -> TypeImpl.<T, V>from(sub).instancesDirect());
    }

    Stream<V> instancesDirect() {
        return vertex().getEdgesOfType(Direction.IN, Schema.EdgeLabel.SHARD).
                map(EdgeElement::source).
                map(source -> vertex().tx().factory().buildShard(source)).
                flatMap(Shard::<V>links);
    }

    @Override
    public Boolean isAbstract() {
        return cachedIsAbstract.get();
    }

    void trackRolePlayers() {
        instances().forEach(concept -> ((ThingImpl<?, ?>) concept).castingsInstance().forEach(
                rolePlayer -> vertex().tx().cache().trackForValidation(rolePlayer)));
    }

    public T play(Role role, boolean required) {
        checkSchemaMutationAllowed();

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
     * @param role The Role Type which the instances of this Type are allowed to play.
     * @return The Type itself.
     */
    public T plays(Role role) {
        return play(role, false);
    }

    /**
     * This is a temporary patch to prevent accidentally disconnecting implicit {@link RelationType}s from their
     * {@link RelationshipEdge}s. This Disconnection happens because {@link RelationType#instances()} depends on the
     * presence of a direct {@link Schema.EdgeLabel#PLAYS} edge between the {@link Type} and the implicit {@link RelationType}.
     * <p>
     * When changing the super you may accidentally cause this disconnection. So we prevent it here.
     */
    //TODO: Remove this when traversing to the instances of an implicit Relationship Type is no longer done via plays edges
    @Override
    boolean changingSuperAllowed(T oldSuperType, T newSuperType) {
        boolean changingSuperAllowed = super.changingSuperAllowed(oldSuperType, newSuperType);
        if (changingSuperAllowed && oldSuperType != null && !Schema.MetaSchema.isMetaLabel(oldSuperType.label())) {
            //noinspection unchecked
            Set<Role> superPlays = oldSuperType.playing().collect(Collectors.toSet());

            //Get everything that this can play bot including the supers
            Set<Role> plays = new HashSet<>(directPlays().keySet());
            subs().flatMap(sub -> TypeImpl.from(sub).directPlays().keySet().stream()).forEach(plays::add);

            superPlays.removeAll(plays);

            //It is possible to be disconnecting from a role which is no longer in use but checking this will take too long
            //So we assume the role is in sure and throw if that is the case
            if (!superPlays.isEmpty() && instancesDirect().findAny().isPresent()) {
                throw TransactionException.changingSuperWillDisconnectRole(oldSuperType, newSuperType, superPlays.iterator().next());
            }

            return true;
        }
        return changingSuperAllowed;
    }

    @Override
    public T unplay(Role role) {
        checkSchemaMutationAllowed();
        deleteEdge(Direction.OUT, Schema.EdgeLabel.PLAYS, (Concept) role);
        cachedDirectPlays.ifPresent(set -> set.remove(role));
        ((RoleImpl) role).deleteCachedDirectPlaysByType(this);

        trackRolePlayers();

        return getThis();
    }

    @Override
    public T unhas(AttributeType attributeType) {
        return unlinkAttribute(attributeType, false);
    }

    @Override
    public T unkey(AttributeType attributeType) {
        return unlinkAttribute(attributeType, true);
    }


    /**
     * Helper method to delete a {@link AttributeType} which is possible linked to this {@link Type}.
     * The link to {@link AttributeType} is removed if <code>attributeToRemove</code> is in the candidate list
     * <code>attributeTypes</code>
     *
     * @param attributeToRemove the {@link AttributeType} to remove
     * @param isKey             a boolean to determine whether the AttributeType to be removed is a KEY to the owning Type
     * @return the {@link Type} itself
     */
    private T unlinkAttribute(AttributeType<?> attributeToRemove, boolean isKey) {
        Stream<AttributeType> attributeTypes = isKey ? keys() : attributes();
        Schema.ImplicitType ownerSchema = isKey ? Schema.ImplicitType.KEY_OWNER : Schema.ImplicitType.HAS_OWNER;
        Schema.ImplicitType valueSchema = isKey ? Schema.ImplicitType.KEY_VALUE : Schema.ImplicitType.HAS_VALUE;
        Schema.ImplicitType relationSchema = isKey ? Schema.ImplicitType.KEY : Schema.ImplicitType.HAS;

        if (attributeTypes.anyMatch(a -> a.equals(attributeToRemove))) {
            Label relationLabel = relationSchema.getLabel(attributeToRemove.label());
            RelationshipTypeImpl relation = vertex().tx().getSchemaConcept(relationLabel);
            if (attributeToRemove.instances().flatMap(Attribute::owners).anyMatch(thing -> thing.type().equals(this))) {
                throw TransactionException.illegalUnhasWithInstance(this.label().getValue(), attributeToRemove.label().getValue(), isKey);
            }

            Label ownerLabel = ownerSchema.getLabel(attributeToRemove.label());
            Role ownerRole = vertex().tx().getSchemaConcept(ownerLabel);

            if (ownerRole == null) {
                throw TransactionException.illegalUnhasNotExist(this.label().getValue(), attributeToRemove.label().getValue(), isKey);
            } else if (!directPlays().keySet().contains(ownerRole)) {
                throw TransactionException.illegalUnhasInherited(this.label().getValue(), attributeToRemove.label().getValue(), isKey);
            }

            unplay(ownerRole);

            // If there are no other Types that own this Attribute, remove the entire implicit relationship
            if (!ownerRole.players().iterator().hasNext()) {
                Label valueLabel = valueSchema.getLabel(attributeToRemove.label());
                Role valueRole = vertex().tx().getSchemaConcept(valueLabel);
                attributeToRemove.unplay(valueRole);

                relation.unrelate(ownerRole);
                relation.unrelate(valueRole);
                ownerRole.delete();
                valueRole.delete();
                relation.delete();
            }
        }

        return getThis();
    }

    /**
     * @param isAbstract Specifies if the concept is abstract (true) or not (false).
     *                   If the concept type is abstract it is not allowed to have any instances.
     * @return The Type itself.
     */
    public T isAbstract(Boolean isAbstract) {
        if (!Schema.MetaSchema.isMetaLabel(label()) && isAbstract && instancesDirect().findAny().isPresent()) {
            throw TransactionException.addingInstancesToAbstractType(this);
        }

        property(Schema.VertexProperty.IS_ABSTRACT, isAbstract);
        cachedIsAbstract.set(isAbstract);

        if (isAbstract) {
            vertex().tx().cache().removeFromValidation(this);
        } else {
            vertex().tx().cache().trackForValidation(this);
        }

        return getThis();
    }

    public T property(Schema.VertexProperty key, Object value) {
        if (!Schema.VertexProperty.CURRENT_LABEL_ID.equals(key)) checkSchemaMutationAllowed();
        vertex().property(key, value);
        return getThis();
    }

    private void updateAttributeRelationHierarchy(AttributeType attributeType, Schema.ImplicitType has, Schema.ImplicitType hasValue, Schema.ImplicitType hasOwner,
                                                  Role ownerRole, Role valueRole, RelationType relationshipType) {
        AttributeType attributeTypeSuper = attributeType.sup();
        Label superLabel = attributeTypeSuper.label();
        Role ownerRoleSuper = vertex().tx().putRoleTypeImplicit(hasOwner.getLabel(superLabel));
        Role valueRoleSuper = vertex().tx().putRoleTypeImplicit(hasValue.getLabel(superLabel));
        RelationType relationshipTypeSuper = vertex().tx().putRelationTypeImplicit(has.getLabel(superLabel)).
                relates(ownerRoleSuper).relates(valueRoleSuper);

        //Create the super type edges from sub role/relations to super roles/relation
        ownerRole.sup(ownerRoleSuper);
        valueRole.sup(valueRoleSuper);
        relationshipType.sup(relationshipTypeSuper);

        if (!Schema.MetaSchema.ATTRIBUTE.getLabel().equals(superLabel)) {
            //Make sure the supertype attribute is linked with the role as well
            ((AttributeTypeImpl) attributeTypeSuper).plays(valueRoleSuper);
            updateAttributeRelationHierarchy(attributeTypeSuper, has, hasValue, hasOwner, ownerRoleSuper, valueRoleSuper, relationshipTypeSuper);
        }

    }

    /**
     * Creates a relation type which allows this type and a {@link Attribute} type to be linked.
     *
     * @param attributeType The {@link AttributeType} which instances of this type should be allowed to play.
     * @param has           the implicit relation type to build
     * @param hasValue      the implicit role type to build for the {@link AttributeType}
     * @param hasOwner      the implicit role type to build for the type
     * @param required      Indicates if the {@link Attribute} is required on the entity
     * @return The {@link Type} itself
     */
    private T has(AttributeType attributeType, Schema.ImplicitType has, Schema.ImplicitType hasValue, Schema.ImplicitType hasOwner, boolean required) {
        Label attributeLabel = attributeType.label();
        Role ownerRole = vertex().tx().putRoleTypeImplicit(hasOwner.getLabel(attributeLabel));
        Role valueRole = vertex().tx().putRoleTypeImplicit(hasValue.getLabel(attributeLabel));
        RelationType relationshipType = vertex().tx().putRelationTypeImplicit(has.getLabel(attributeLabel)).
                relates(ownerRole).
                relates(valueRole);

        //this plays ownerRole;
        this.play(ownerRole, required);
        //TODO: Use explicit cardinality of 0-1 rather than just false
        //attributeType plays valueRole;
        ((AttributeTypeImpl) attributeType).play(valueRole, false);

        updateAttributeRelationHierarchy(attributeType, has, hasValue, hasOwner, ownerRole, valueRole, relationshipType);

        return getThis();
    }

    @Override
    public T has(AttributeType attributeType) {
        checkLinkAttributeIsLegal(Schema.ImplicitType.KEY_OWNER, attributeType);
        return has(attributeType, Schema.ImplicitType.HAS, Schema.ImplicitType.HAS_VALUE, Schema.ImplicitType.HAS_OWNER, false);
    }

    @Override
    public T key(AttributeType attributeType) {
        checkLinkAttributeIsLegal(Schema.ImplicitType.HAS_OWNER, attributeType);
        return has(attributeType, Schema.ImplicitType.KEY, Schema.ImplicitType.KEY_VALUE, Schema.ImplicitType.KEY_OWNER, true);
    }

    private void checkLinkAttributeIsLegal(Schema.ImplicitType implicitType, AttributeType attributeType) {
        checkSchemaMutationAllowed();
        checkIfHasTargetMeta(attributeType);
        checkNonOverlapOfImplicitRelations(implicitType, attributeType);
    }

    private void checkIfHasTargetMeta(AttributeType attributeType) {
        //Check if attribute type is the meta
        if (Schema.MetaSchema.ATTRIBUTE.getLabel().equals(attributeType.label())) {
            throw TransactionException.metaTypeImmutable(attributeType.label());
        }
    }

    /**
     * Checks if the provided {@link AttributeType} is already used in an other implicit relation.
     *
     * @param implicitType  The implicit relation to check against.
     * @param attributeType The {@link AttributeType} which should not be in that implicit relation
     * @throws TransactionException when the {@link AttributeType} is already used in another implicit relation
     */
    private void checkNonOverlapOfImplicitRelations(Schema.ImplicitType implicitType, AttributeType attributeType) {
        if (attributes(implicitType).anyMatch(rt -> rt.equals(attributeType))) {
            throw TransactionException.duplicateHas(this, attributeType);
        }
    }

    public static <X extends Type, Y extends Thing> TypeImpl<X, Y> from(Type type) {
        //noinspection unchecked
        return (TypeImpl<X, Y>) type;
    }
}
