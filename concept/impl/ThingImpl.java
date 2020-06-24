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
import grakn.core.kb.concept.api.Casting;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.concept.manager.ConceptNotificationChannel;
import grakn.core.kb.concept.structure.EdgeElement;
import grakn.core.kb.concept.structure.VertexElement;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
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
        castingsInstance().forEach(casting -> {
            Relation relation = casting.getRelation();
            Role role = casting.getRole();
            relation.unassign(role, this);
        });

        deleteAttributeOwnerships();

        if (!isDeleted()) {
            // must happen before deleteNode() so we can access properties on the vertex
            conceptNotificationChannel.thingDeleted(this);
        }

        deleteNode();
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
    public Stream<Attribute<?>> keys(AttributeType<?>... attributeTypes) {
        List<AttributeType<?>> keyTypes = type().keys().collect(Collectors.toList());
        if (attributeTypes.length == 0) {
            return attributes(keyTypes.toArray(new AttributeType<?>[0]));
        }

        Set<AttributeType<?>> attributeTypeSet = new HashSet<>();
        Collections.addAll(attributeTypeSet, attributeTypes);

        attributeTypeSet.retainAll(keyTypes);

        if (attributeTypeSet.isEmpty()) return Stream.empty();
        else return attributes(attributeTypeSet.toArray(new AttributeType<?>[0]));
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

    @Override
    public Stream<Attribute<?>> attributes(AttributeType<?>... attributeTypes) {
        Set<AttributeType<?>> completeAttributeTypes = new HashSet<>(Arrays.asList(attributeTypes));
        if (completeAttributeTypes.isEmpty()) {
            completeAttributeTypes.add(conceptManager.getMetaAttributeType());
        }

        Set<AttributeType<?>> typesWithSubs = completeAttributeTypes.stream()
                .flatMap(AttributeType::subs)
                .collect(toSet());

        Stream<Attribute<?>> attributesOwned = neighbours(Direction.OUT, Schema.EdgeLabel.ATTRIBUTE)
                .filter(neighbour -> typesWithSubs.contains(neighbour.asThing().type()))
                .map(Concept::asAttribute);

        return attributesOwned;
    }

    /**
     * @param roles An optional parameter which allows you to specify the role of the relations you wish to retrieve.
     * @return A set of Relations which the concept instance takes part in, optionally constrained by the Role Type.
     */
    @Override
    public Stream<Relation> relations(Role... roles) {
        Set<Integer> roleIds = Arrays.stream(roles).map(role -> role.labelId().getValue()).collect(Collectors.toSet());
        Stream<VertexElement> relationVertices = vertex().relations(roleIds);
        return relationVertices.map(vertexElement -> conceptManager.buildConcept(vertexElement));
    }

    private void deleteAttributeOwnerships() {
        // delete ownerships of this Thing
        if (isAttribute()) {
            vertex().getEdgesOfType(Direction.IN, Schema.EdgeLabel.ATTRIBUTE)
                    .forEach(edge -> {
                        Thing owner = conceptManager.buildConcept(edge.target()).asThing();
                        conceptNotificationChannel.hasAttributeRemoved(owner, this.asAttribute(), isInferred());
                        edge.delete();
                    });
        }
        vertex().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.ATTRIBUTE)
                .forEach(edge -> {
                    Attribute attribute = conceptManager.buildConcept(edge.target()).asAttribute();
                    conceptNotificationChannel.hasAttributeRemoved(this, attribute, attribute.isInferred());
                    edge.delete();
                });
    }

    @Override
    public Stream<Role> roles() {
        return castingsInstance().map(Casting::getRole);
    }

    @Override
    public T has(Attribute attribute) {
        attributeOwnership(attribute, false);
        return getThis();
    }

    @Override
    public void attributeInferred(Attribute attribute) {
        attributeOwnership(attribute, true);
    }

    private void attributeOwnership(Attribute attribute, boolean isInferred) {
        if (type().has().noneMatch(attribute.type()::equals)) {
            throw GraknConceptException.hasNotAllowed(this, attribute);
        }

        if (this.attributes(attribute.type()).noneMatch(attribute::equals)) {
            EdgeElement attributeEdge = putEdge(AttributeImpl.from(attribute), Schema.EdgeLabel.ATTRIBUTE);
            attributeEdge.property(Schema.EdgeProperty.ATTRIBUTE_OWNED_LABEL_ID, attribute.type().labelId().getValue());
            if (isInferred) {
                attributeEdge.property(Schema.EdgeProperty.IS_INFERRED, true);
            }
            conceptManager.createHasAttribute(this, attribute, isInferred);
        }

    }

    @Override
    public T unhas(Attribute attribute) {
        // delete attribute ownerships between this Thing and the Attribute
        // TODO may need to be able to limit the number of times the edge is removed if there are multiple - this removes all
        Optional<EdgeElement> edgeElement = vertex().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.ATTRIBUTE)
                .filter(edge -> edge.target().equals(ConceptVertex.from(attribute).vertex()))
                .findAny();

        if (edgeElement.isPresent()) {
            EdgeElement edge = edgeElement.get();
            boolean isInferred = edge.propertyBoolean(Schema.EdgeProperty.IS_INFERRED);
            edge.delete();
            conceptNotificationChannel.hasAttributeRemoved(this, attribute, isInferred);
        }

        return getThis();
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
