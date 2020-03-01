/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.concept.impl;

import grakn.core.concept.cache.ConceptCache;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.concept.manager.ConceptNotificationChannel;
import grakn.core.kb.concept.structure.EdgeElement;
import grakn.core.kb.concept.structure.Shard;
import grakn.core.kb.concept.structure.VertexElement;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A Type represents any ontological element in the graph.
 * Types are used to model the behaviour of Thing and how they relate to each other.
 * They also aid in categorising Thing to different types.
 *
 * @param <T> The leaf interface of the object concept. For example an EntityType or RelationType
 * @param <V> The instance of this type. For example Entity or Relation
 */
public class TypeImpl<T extends Type, V extends Thing> extends SchemaConceptImpl<T> implements Type {

    private final ConceptCache<Boolean> cachedIsAbstract = new ConceptCache<>(() -> vertex().propertyBoolean(Schema.VertexProperty.IS_ABSTRACT));

    //This cache is different in order to keep track of which plays are required
    private final ConceptCache<Map<Role, Boolean>> cachedDirectPlays = new ConceptCache<>(() -> {
        Map<Role, Boolean> roleTypes = new HashMap<>();

        vertex().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.PLAYS).forEach(edge -> {
            Role role = conceptManager.buildConcept(edge.target());
            Boolean required = edge.propertyBoolean(Schema.EdgeProperty.REQUIRED);
            roleTypes.put(role, required);
        });

        return roleTypes;
    });

    public TypeImpl(VertexElement vertexElement, ConceptManager conceptManager, ConceptNotificationChannel conceptNotificationChannel) {
        super(vertexElement, conceptManager, conceptNotificationChannel);
    }

    public static <X extends Type, Y extends Thing> TypeImpl<X, Y> from(Type type) {
        //noinspection unchecked
        return (TypeImpl<X, Y>) type;
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

        //NB: use distinct as roles from different types from the hierarchy can overlap
        return Stream.concat(allRoles, superSet).distinct();
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
                    return conceptManager.getAttributeType(attributeTypeLabel);
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
        return vertex().getEdgesOfType(Direction.IN, Schema.EdgeLabel.SHARD)
                .map(EdgeElement::source)
                .map(VertexElement::asShard)
                .flatMap(Shard::links)
                .map(shardTargetVertex -> conceptManager.buildConcept(shardTargetVertex));
    }

    @Override
    public Boolean isAbstract() {
        return cachedIsAbstract.get();
    }

    @Override
    public T play(Role role, boolean required) {
        checkSchemaMutationAllowed();

        //Update the internal cache of role types played
        cachedDirectPlays.ifCached(map -> map.put(role, required));

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
    @Override
    public T plays(Role role) {
        return play(role, false);
    }

    /**
     * This is a temporary patch to prevent accidentally disconnecting implicit RelationTypes from their
     * RelationEdges. This Disconnection happens because RelationType.instances() depends on the
     * presence of a direct Schema.EdgeLabel#PLAYS edge between the Type and the implicit RelationType.
     * When changing the super you may accidentally cause this disconnection. So we prevent it here.
     */
    //TODO: Remove this when traversing to the instances of an implicit RelationType is no longer done via plays edges
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
                throw GraknConceptException.changingSuperWillDisconnectRole(oldSuperType, newSuperType, superPlays.iterator().next());
            }

            return true;
        }
        return changingSuperAllowed;
    }

    @Override
    public T unplay(Role role) {
        checkSchemaMutationAllowed();
        deleteEdge(Direction.OUT, Schema.EdgeLabel.PLAYS, (Concept) role);
        cachedDirectPlays.ifCached(set -> set.remove(role));
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
     * Helper method to delete a AttributeType which is possible linked to this Type.
     * The link to AttributeType is removed if <code>attributeToRemove</code> is in the candidate list
     * <code>attributeTypes</code>
     *
     * @param attributeToRemove the AttributeType to remove
     * @param isKey             a boolean to determine whether the AttributeType to be removed is a KEY to the owning Type
     * @return the Type itself
     */
    private T unlinkAttribute(AttributeType<?> attributeToRemove, boolean isKey) {
        Stream<AttributeType> attributeTypes = isKey ? keys() : attributes();
        Schema.ImplicitType ownerSchema = isKey ? Schema.ImplicitType.KEY_OWNER : Schema.ImplicitType.HAS_OWNER;
        Schema.ImplicitType valueSchema = isKey ? Schema.ImplicitType.KEY_VALUE : Schema.ImplicitType.HAS_VALUE;
        Schema.ImplicitType relationSchema = isKey ? Schema.ImplicitType.KEY : Schema.ImplicitType.HAS;

        if (attributeTypes.anyMatch(a -> a.equals(attributeToRemove))) {
            Label relationLabel = relationSchema.getLabel(attributeToRemove.label());
            RelationTypeImpl relation = conceptManager.getSchemaConcept(relationLabel);
            if (attributeToRemove.instances().flatMap(Attribute::owners).anyMatch(thing -> thing.type().equals(this))) {
                throw GraknConceptException.illegalUnhasWithInstance(this.label().getValue(), attributeToRemove.label().getValue(), isKey);
            }

            Label ownerLabel = ownerSchema.getLabel(attributeToRemove.label());
            Role ownerRole = conceptManager.getSchemaConcept(ownerLabel);

            if (ownerRole == null) {
                throw GraknConceptException.illegalUnhasNotExist(this.label().getValue(), attributeToRemove.label().getValue(), isKey);
            } else if (!directPlays().keySet().contains(ownerRole)) {
                throw GraknConceptException.illegalUnhasInherited(this.label().getValue(), attributeToRemove.label().getValue(), isKey);
            }

            unplay(ownerRole);

            // If there are no other Types that own this Attribute, remove the entire implicit relation
            if (!ownerRole.players().iterator().hasNext()) {
                Label valueLabel = valueSchema.getLabel(attributeToRemove.label());
                Role valueRole = conceptManager.getSchemaConcept(valueLabel);
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
            throw GraknConceptException.addingInstancesToAbstractType(this);
        }

        property(Schema.VertexProperty.IS_ABSTRACT, isAbstract);
        cachedIsAbstract.set(isAbstract);

        conceptNotificationChannel.conceptSetAbstract(this, isAbstract);

        return getThis();
    }

    public T property(Schema.VertexProperty key, Object value) {
        if (!Schema.VertexProperty.CURRENT_LABEL_ID.equals(key)) checkSchemaMutationAllowed();
        vertex().property(key, value);
        return getThis();
    }

    private void updateAttributeRelationHierarchy(AttributeType attributeType, Schema.ImplicitType has, Schema.ImplicitType hasValue, Schema.ImplicitType hasOwner,
                                                  Role ownerRole, Role valueRole, RelationType relationType) {
        AttributeType attributeTypeSuper = attributeType.sup();
        Label superLabel = attributeTypeSuper.label();
        Role ownerRoleSuper = conceptManager.getRole(hasOwner.getLabel(superLabel).getValue());
        // create implicit roles and relations if required
        if (ownerRoleSuper == null) {
            ownerRoleSuper = conceptManager.createImplicitRole(hasOwner.getLabel(superLabel));
        }
        Role valueRoleSuper = conceptManager.getRole(hasValue.getLabel(superLabel).getValue());
        if (valueRoleSuper == null) {
            valueRoleSuper = conceptManager.createImplicitRole(hasValue.getLabel(superLabel));
        }

        RelationType relationTypeSuper = conceptManager.getRelationType(has.getLabel(superLabel).getValue());
        if (relationTypeSuper == null) {
            relationTypeSuper = conceptManager.createImplicitRelationType(has.getLabel(superLabel));
        }
        relationTypeSuper.relates(ownerRoleSuper).relates(valueRoleSuper);

        //Create the super type edges from sub role/relations to super roles/relation
        ownerRole.sup(ownerRoleSuper);
        valueRole.sup(valueRoleSuper);
        relationType.sup(relationTypeSuper);

        if (!Schema.MetaSchema.ATTRIBUTE.getLabel().equals(superLabel)) {
            //Make sure the supertype attribute is linked with the role as well
            ((AttributeTypeImpl) attributeTypeSuper).plays(valueRoleSuper);
            updateAttributeRelationHierarchy(attributeTypeSuper, has, hasValue, hasOwner, ownerRoleSuper, valueRoleSuper, relationTypeSuper);
        }
    }

    /**
     * Creates a relation type which allows this type and a Attribute type to be linked.
     *
     * @param attributeType The AttributeType which instances of this type should be allowed to play.
     * @param has           the implicit relation type to build
     * @param hasValue      the implicit role type to build for the AttributeType
     * @param hasOwner      the implicit role type to build for the type
     * @param required      Indicates if the Attribute is required on the entity
     * @return The Type itself
     */
    private T has(AttributeType attributeType, Schema.ImplicitType has, Schema.ImplicitType hasValue, Schema.ImplicitType hasOwner, boolean required) {
        Label attributeLabel = attributeType.label();
        Role ownerRole = conceptManager.getRole(hasOwner.getLabel(attributeLabel).getValue());
        if (ownerRole == null) {
            ownerRole = conceptManager.createImplicitRole(hasOwner.getLabel(attributeLabel));
        }
        Role valueRole = conceptManager.getRole(hasValue.getLabel(attributeLabel).getValue());
        if (valueRole == null) {
            valueRole = conceptManager.createImplicitRole(hasValue.getLabel(attributeLabel));
        }

        RelationType relationType = conceptManager.getRelationType(has.getLabel(attributeLabel).getValue());
        if (relationType == null) {
            relationType = conceptManager.createImplicitRelationType(has.getLabel(attributeLabel));
        }

        relationType.relates(ownerRole).relates(valueRole);

        //this plays ownerRole;
        this.play(ownerRole, required);
        //TODO: Use explicit cardinality of 0-1 rather than just false
        //attributeType plays valueRole;
        ((AttributeTypeImpl) attributeType).play(valueRole, false);

        updateAttributeRelationHierarchy(attributeType, has, hasValue, hasOwner, ownerRole, valueRole, relationType);

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
            throw GraknConceptException.metaTypeImmutable(attributeType.label());
        }
    }

    /**
     * Checks if the provided AttributeType is already used in an other implicit relation.
     *
     * @param implicitType  The implicit relation to check against.
     * @param attributeType The AttributeType which should not be in that implicit relation
     * @throws GraknConceptException when the AttributeType is already used in another implicit relation
     */
    private void checkNonOverlapOfImplicitRelations(Schema.ImplicitType implicitType, AttributeType attributeType) {
        if (attributes(implicitType).anyMatch(rt -> rt.equals(attributeType))) {
            throw GraknConceptException.duplicateHas(this, attributeType);
        }
    }

    @Override
    void trackRolePlayers() {
        // this method needs to be implemented here for the single case when trying to instantiate the top level
        // meta Thing concept.
        // In theory this class should be abstract, and only use the overriding
        // implementations of this method - EntityType, AttributeType, RelationType
        // each of these subclasses contains an actual implementation of trackRolePlayers()

        // this method is empty because it is only used when instanting top level Concept - which can never have role
        // players or play roles.
    }

    @Override
    public Long getCount() {
        Long count = vertex().property(Schema.VertexProperty.INSTANCE_COUNT);
        if (count != null) {
            return count;
        }
        return 0L;
    }

    public void writeCount(Long count) {
        vertex().property(Schema.VertexProperty.INSTANCE_COUNT, count);
    }

}
