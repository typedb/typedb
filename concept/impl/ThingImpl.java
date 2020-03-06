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

import com.google.common.collect.Sets;
import grakn.core.concept.cache.ConceptCache;
import grakn.core.concept.structure.CastingImpl;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.LabelId;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.concept.manager.ConceptNotificationChannel;
import grakn.core.kb.concept.structure.Casting;
import grakn.core.kb.concept.structure.EdgeElement;
import grakn.core.kb.concept.structure.VertexElement;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

/**
 * A data instance in the graph belonging to a specific Type
 * Instances represent data in the graph.
 * Every instance belongs to a Type which serves as a way of categorising them.
 * Instances can relate to one another via Relation
 *
 * @param <T> The leaf interface of the object concept which extends Thing.
 *            For example Entity or Relation.
 * @param <V> The type of the concept which extends Type of the concept.
 *            For example EntityType or RelationType
 */
public abstract class ThingImpl<T extends Thing, V extends Type> extends ConceptImpl implements Thing {

    private Boolean isInferred = null;

    private final ConceptCache<V> cachedType = new ConceptCache<>(() -> {
        Optional<V> type = vertex().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.ISA)
                .map(EdgeElement::target)
                .flatMap(edge -> edge.getEdgesOfType(Direction.OUT, Schema.EdgeLabel.SHARD))
                .map(EdgeElement::target)
                .map(concept -> conceptManager.<V>buildConcept(concept))
                .findAny();

        return type.orElseThrow(() -> GraknConceptException.noType(this));
    });

    ThingImpl(VertexElement vertexElement, ConceptManager conceptManager, ConceptNotificationChannel conceptNotificationChannel) {
        super(vertexElement, conceptManager, conceptNotificationChannel);
    }

    public boolean isInferred() {
        //NB: might be over the top to cache it
        if (isInferred == null) {
            isInferred = vertex().propertyBoolean(Schema.VertexProperty.IS_INFERRED);
        }
        return isInferred;
    }

    /**
     * Deletes the concept as an Thing
     */
    @Override
    public void delete() {
        //Remove links to relations and return them
        Set<Relation> relations = castingsInstance().map(casting -> {
            Relation relation = casting.getRelation();
            Role role = casting.getRole();
            relation.unassign(role, this);
            return relation;
        }).collect(toSet());

        if (!isDeleted())  {
            // must happen before deleteNode() so we can access properties on the vertex
            conceptNotificationChannel.thingDeleted(this);
        }

        this.edgeRelations().forEach(Concept::delete);

        deleteNode();

        relations.forEach(relation -> {
            //NB: this only deletes reified implicit relations
            if (relation.type().isImplicit()) {
                relation.delete();
            } else {
                RelationImpl rel = (RelationImpl) relation;
                rel.cleanUp();
            }
        });
    }

    /**
     * This index is used by concepts such as casting and relations to speed up internal lookups
     *
     * @return The inner index value of some concepts.
     */
    public String getIndex() {
        return vertex().property(Schema.VertexProperty.INDEX);
    }

    @Override
    public Stream<Attribute<?>> attributes(AttributeType... attributeTypes) {
        Set<ConceptId> attributeTypesIds = Arrays.stream(attributeTypes).map(Concept::id).collect(toSet());
        return attributes(getShortcutNeighbours(true), attributeTypesIds);
    }

    @Override
    public Stream<Attribute<?>> keys(AttributeType... attributeTypes) {
        Set<ConceptId> attributeTypesIds = Arrays.stream(attributeTypes).map(Concept::id).collect(toSet());
        Set<ConceptId> keyTypeIds = type().keys().map(Concept::id).collect(toSet());

        if (!attributeTypesIds.isEmpty()) {
            keyTypeIds = Sets.intersection(attributeTypesIds, keyTypeIds);
        }

        if (keyTypeIds.isEmpty()) return Stream.empty();

        return attributes(getShortcutNeighbours(true), keyTypeIds);
    }

    /**
     * Helper class which filters a Stream of Attribute to those of a specific set of AttributeType.
     *
     * @param conceptStream     The Stream to filter
     * @param attributeTypesIds The AttributeType ConceptIds to filter to.
     * @return the filtered stream
     */
    private <X extends Concept> Stream<Attribute<?>> attributes(Stream<X> conceptStream, Set<ConceptId> attributeTypesIds) {
        Stream<Attribute<?>> attributeStream = conceptStream.
                filter(concept -> concept.isAttribute() && !this.equals(concept)).
                map(Concept::asAttribute);

        if (!attributeTypesIds.isEmpty()) {
            attributeStream = attributeStream.filter(attribute -> attributeTypesIds.contains(attribute.type().id()));
        }

        return attributeStream;
    }

    /**
     * Castings are retrieved from the perspective of the Thing which is a role player in a Relation
     *
     * @return All the Casting which this instance is cast into the role
     */
    public Stream<Casting> castingsInstance() {
        return vertex().getEdgesOfType(Direction.IN, Schema.EdgeLabel.ROLE_PLAYER)
                .map(edge -> CastingImpl.withThing(edge, this, conceptManager));
    }

    private Set<Integer> implicitLabelsToIds(Set<Label> labels, Set<Schema.ImplicitType> implicitTypes) {
        return labels.stream()
                .flatMap(label -> implicitTypes.stream().map(it -> it.getLabel(label)))
                .map(label -> conceptManager.convertToId(label))
                .filter(id -> !id.equals(LabelId.invalid()))
                .map(LabelId::getValue)
                .collect(toSet());
    }

    <X extends Thing> Stream<X> getShortcutNeighbours(boolean ownerToValueOrdering, AttributeType... attributeTypes) {
        Set<AttributeType> completeAttributeTypes = new HashSet<>(Arrays.asList(attributeTypes));
        if (completeAttributeTypes.isEmpty()) {
            completeAttributeTypes.add(conceptManager.getMetaAttributeType());
        }

        Set<Label> attributeHierarchyLabels = completeAttributeTypes.stream()
                .flatMap(t -> (Stream<AttributeType>) t.subs())
                .map(SchemaConcept::label)
                .collect(toSet());

        Set<Integer> ownerRoleIds = implicitLabelsToIds(
                attributeHierarchyLabels,
                Sets.newHashSet(
                        Schema.ImplicitType.HAS_OWNER,
                        Schema.ImplicitType.KEY_OWNER
                ));
        Set<Integer> valueRoleIds = implicitLabelsToIds(
                attributeHierarchyLabels,
                Sets.newHashSet(
                        Schema.ImplicitType.HAS_VALUE,
                        Schema.ImplicitType.KEY_VALUE
                ));

        Stream<VertexElement> shortcutNeighbors = vertex().getShortcutNeighbors(ownerRoleIds, valueRoleIds, ownerToValueOrdering);
        return shortcutNeighbors.map(vertexElement -> conceptManager.buildConcept(vertexElement));

    }

    /**
     * @param roles An optional parameter which allows you to specify the role of the relations you wish to retrieve.
     * @return A set of Relations which the concept instance takes part in, optionally constrained by the Role Type.
     */
    @Override
    public Stream<Relation> relations(Role... roles) {
        return Stream.concat(reifiedRelations(roles), edgeRelations(roles));
    }

    private Stream<Relation> reifiedRelations(Role... roles) {
        Stream<VertexElement> reifiedRelationVertices = vertex().reifiedRelations(roles);
        return reifiedRelationVertices.map(vertexElement -> conceptManager.buildConcept(vertexElement));
    }

    private Stream<Relation> edgeRelations(Role... roles) {
        Set<Role> roleSet = new HashSet<>(Arrays.asList(roles));
        // TODO move this into the AbstractElement and ElementFactory as well
        Stream<EdgeElement> stream = vertex().getEdgesOfType(Direction.BOTH, Schema.EdgeLabel.ATTRIBUTE);

        if (!roleSet.isEmpty()) {
            stream = stream.filter(edge -> {
                Set<Role> edgeRoles = new HashSet<>();
                edgeRoles.add(conceptManager.getSchemaConcept(LabelId.of(edge.property(Schema.EdgeProperty.RELATION_ROLE_OWNER_LABEL_ID))));
                if (this.isAttribute()){
                    edgeRoles.add(conceptManager.getSchemaConcept(LabelId.of(edge.property(Schema.EdgeProperty.RELATION_ROLE_VALUE_LABEL_ID))));
                }
                return !Sets.intersection(roleSet, edgeRoles).isEmpty();
            });
        }

        return stream.map(edge -> conceptManager.buildRelation(edge));
    }

    @Override
    public Stream<Role> roles() {
        return castingsInstance().map(Casting::getRole);
    }

    @Override
    public T has(Attribute attribute) {
        relhas(attribute);
        return getThis();
    }

    @Override
    public Relation attributeInferred(Attribute attribute) {
        return attributeRelation(attribute, true);
    }

    @Override
    public Relation relhas(Attribute attribute) {
        return attributeRelation(attribute, false);
    }

    private Relation attributeRelation(Attribute attribute, boolean isInferred) {
        Schema.ImplicitType has = Schema.ImplicitType.HAS;
        Schema.ImplicitType hasValue = Schema.ImplicitType.HAS_VALUE;
        Schema.ImplicitType hasOwner = Schema.ImplicitType.HAS_OWNER;

        //Is this attribute a key to me?
        if (type().keys().anyMatch(key -> key.equals(attribute.type()))) {
            has = Schema.ImplicitType.KEY;
            hasValue = Schema.ImplicitType.KEY_VALUE;
            hasOwner = Schema.ImplicitType.KEY_OWNER;
        }

        Label label = attribute.type().label();
        RelationType hasAttribute = conceptManager.getSchemaConcept(has.getLabel(label));
        Role hasAttributeOwner = conceptManager.getSchemaConcept(hasOwner.getLabel(label));
        Role hasAttributeValue = conceptManager.getSchemaConcept(hasValue.getLabel(label));

        if (hasAttribute == null || hasAttributeOwner == null || hasAttributeValue == null || type().playing().noneMatch(play -> play.equals(hasAttributeOwner))) {
            throw GraknConceptException.hasNotAllowed(this, attribute);
        }

        EdgeElement attributeEdge = addEdge(AttributeImpl.from(attribute), Schema.EdgeLabel.ATTRIBUTE);
        if (isInferred) attributeEdge.property(Schema.EdgeProperty.IS_INFERRED, true);

        return conceptManager.createHasAttributeRelation(attributeEdge, hasAttribute, hasAttributeOwner, hasAttributeValue, isInferred);
    }

    @Override
    public T unhas(Attribute attribute) {
        Role roleHasOwner = conceptManager.getSchemaConcept(Schema.ImplicitType.HAS_OWNER.getLabel(attribute.type().label()));
        Role roleKeyOwner = conceptManager.getSchemaConcept(Schema.ImplicitType.KEY_OWNER.getLabel(attribute.type().label()));

        Role roleHasValue = conceptManager.getSchemaConcept(Schema.ImplicitType.HAS_VALUE.getLabel(attribute.type().label()));
        Role roleKeyValue = conceptManager.getSchemaConcept(Schema.ImplicitType.KEY_VALUE.getLabel(attribute.type().label()));

        Stream<Relation> relations = relations(filterNulls(roleHasOwner, roleKeyOwner));
        relations.filter(relation -> {
            Stream<Thing> rolePlayers = relation.rolePlayers(filterNulls(roleHasValue, roleKeyValue));
            return rolePlayers.anyMatch(rolePlayer -> rolePlayer.equals(attribute));
        }).forEach(Concept::delete);

        return getThis();
    }

    /**
     * Returns an array with all the nulls filtered out.
     */
    private Role[] filterNulls(Role... roles) {
        return Arrays.stream(roles).filter(Objects::nonNull).toArray(Role[]::new);
    }

    /**
     * @return The type of the concept casted to the correct interface
     */
    public V type() {
        return cachedType.get();
    }

    /**
     * @param type The type of this concept
     */
    public void type(TypeImpl type) {
        if (type != null) {
            //noinspection unchecked
            cachedType.set((V) type); //We cache the type early because it turns out we use it EVERY time. So this prevents many db reads
            type.currentShard().link(vertex());
            setInternalType(type);
        }
    }

    /**
     * @param type Set property on vertex that stores Type label - this is needed by Analytics
     */
    private void setInternalType(Type type) {
        vertex().property(Schema.VertexProperty.THING_TYPE_LABEL_ID, type.labelId().getValue());
    }

}
