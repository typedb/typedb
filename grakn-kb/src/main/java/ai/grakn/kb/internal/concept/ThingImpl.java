/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.kb.internal.concept;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.kb.internal.cache.Cache;
import ai.grakn.kb.internal.cache.Cacheable;
import ai.grakn.kb.internal.structure.Casting;
import ai.grakn.kb.internal.structure.EdgeElement;
import ai.grakn.kb.internal.structure.VertexElement;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <p>
 *     A data instance in the graph belonging to a specific {@link Type}
 * </p>
 *
 * <p>
 *     Instances represent data in the graph.
 *     Every instance belongs to a {@link Type} which serves as a way of categorising them.
 *     Instances can relate to one another via {@link Relationship}
 * </p>
 *
 * @author fppt
 *
 * @param <T> The leaf interface of the object concept which extends {@link Thing}.
 *           For example {@link ai.grakn.concept.Entity} or {@link Relationship}.
 * @param <V> The type of the concept which extends {@link Type} of the concept.
 *           For example {@link ai.grakn.concept.EntityType} or {@link RelationshipType}
 */
public abstract class ThingImpl<T extends Thing, V extends Type> extends ConceptImpl implements Thing {
    private final Cache<Label> cachedInternalType = Cache.createTxCache(this, Cacheable.label(), () -> {
        int typeId = vertex().property(Schema.VertexProperty.THING_TYPE_LABEL_ID);
        Optional<Type> type = vertex().tx().getConcept(Schema.VertexProperty.LABEL_ID, typeId);
        return type.orElseThrow(() -> GraknTxOperationException.missingType(getId())).getLabel();
    });

    private final Cache<V> cachedType = Cache.createTxCache(this, Cacheable.concept(), () -> {
        Optional<V> type = vertex().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.ISA).
                map(EdgeElement::target).
                flatMap(edge -> edge.getEdgesOfType(Direction.OUT, Schema.EdgeLabel.SHARD)).
                map(EdgeElement::target).
                map(concept -> vertex().tx().factory().<V>buildConcept(concept)).
                findAny();

        return type.orElseThrow(() -> GraknTxOperationException.noType(this));
    });

    ThingImpl(VertexElement vertexElement) {
        super(vertexElement);
    }

    ThingImpl(VertexElement vertexElement, V type) {
        this(vertexElement);
        type((TypeImpl) type);
        track();
    }

    /**
     * This {@link Thing} gets tracked for validation only if it has keys which need to be checked.
     */
    private void track(){
        if(type().keys().findAny().isPresent()){
            vertex().tx().txCache().trackForValidation(this);
        }
    }

    public boolean isInferred(){
        return vertex().propertyBoolean(Schema.VertexProperty.IS_INFERRED);
    }

    /**
     * Deletes the concept as an Thing
     */
    @Override
    public void delete() {
        //Remove links to relationships and return them
        Set<Relationship> relationships = castingsInstance().map(casting -> {
            Relationship relationship = casting.getRelationship();
            Role role = casting.getRole();
            relationship.removeRolePlayer(role, this);
            return relationship;
        }).collect(Collectors.toSet());

        vertex().tx().txCache().removedInstance(type().getId());
        deleteNode();

        relationships.forEach(relation -> {
            if(relation.type().isImplicit()){//For now implicit relationships die
                relation.delete();
            } else {
                RelationshipImpl rel = (RelationshipImpl) relation;
                rel.cleanUp();
            }
        });
    }

    /**
     * This index is used by concepts such as casting and relations to speed up internal lookups
     * @return The inner index value of some concepts.
     */
    public String getIndex(){
        return vertex().property(Schema.VertexProperty.INDEX);
    }

    @Override
    public Stream<Attribute<?>> attributes(AttributeType... attributeTypes) {
        Set<ConceptId> attributeTypesIds = Arrays.stream(attributeTypes).map(Concept::getId).collect(Collectors.toSet());
        return attributes(getShortcutNeighbours(), attributeTypesIds);
    }

    @Override
    public Stream<Attribute<?>> keys(AttributeType... attributeTypes){
        Set<ConceptId> attributeTypesIds = Arrays.stream(attributeTypes).map(Concept::getId).collect(Collectors.toSet());
        Set<ConceptId> keyTypeIds = type().keys().map(Concept::getId).collect(Collectors.toSet());

        if(!attributeTypesIds.isEmpty()){
            keyTypeIds = Sets.intersection(attributeTypesIds, keyTypeIds);
        }

        if(keyTypeIds.isEmpty()) return Stream.empty();

        return attributes(getShortcutNeighbours(), keyTypeIds);
    }

    /**
     * Helper class which filters a {@link Stream} of {@link Attribute} to those of a specific set of {@link AttributeType}.
     *
     * @param conceptStream The {@link Stream} to filter
     * @param attributeTypesIds The {@link AttributeType} {@link ConceptId}s to filter to.
     * @return the filtered stream
     */
    private <X extends Concept> Stream<Attribute<?>> attributes(Stream<X> conceptStream, Set<ConceptId> attributeTypesIds){
        Stream<Attribute<?>> attributeStream = conceptStream.
                filter(concept -> concept.isAttribute() && !this.equals(concept)).
                map(Concept::asAttribute);

        if(!attributeTypesIds.isEmpty()){
            attributeStream = attributeStream.filter(attribute -> attributeTypesIds.contains(attribute.type().getId()));
        }

        return attributeStream;
    }

    /**
     * Castings are retrieved from the perspective of the {@link Thing} which is a role player in a {@link Relationship}
     *
     * @return All the {@link Casting} which this instance is cast into the role
     */
    Stream<Casting> castingsInstance(){
        return vertex().getEdgesOfType(Direction.IN, Schema.EdgeLabel.ROLE_PLAYER).
                map(edge -> Casting.withThing(edge, this));
    }

    <X extends Thing> Stream<X> getShortcutNeighbours(){
        GraphTraversal<Object, Vertex> shortcutTraversal = __.inE(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).
                as("edge").
                outV().
                outE(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).
                where(P.neq("edge")).
                inV();

        GraphTraversal<Object, Vertex> attributeEdgeTraversal = __.outE(Schema.EdgeLabel.ATTRIBUTE.getLabel()).inV();

        //noinspection unchecked
        return vertex().tx().getTinkerTraversal().V().
                has(Schema.VertexProperty.ID.name(), getId().getValue()).
                union(shortcutTraversal, attributeEdgeTraversal).toStream().
                map(vertex -> vertex().tx().<X>buildConcept(vertex));
    }

    /**
     *
     * @param roles An optional parameter which allows you to specify the role of the relations you wish to retrieve.
     * @return A set of Relations which the concept instance takes part in, optionally constrained by the Role Type.
     */
    @Override
    public Stream<Relationship> relationships(Role... roles) {
        return Stream.concat(reifiedRelations(roles), edgeRelations(roles));
    }

    private Stream<Relationship> reifiedRelations(Role... roles){
        GraphTraversal<Vertex, Vertex> traversal = vertex().tx().getTinkerTraversal().V().
                has(Schema.VertexProperty.ID.name(), getId().getValue());

        if(roles.length == 0){
            traversal.in(Schema.EdgeLabel.ROLE_PLAYER.getLabel());
        } else {
            Set<Integer> roleTypesIds = Arrays.stream(roles).map(r -> r.getLabelId().getValue()).collect(Collectors.toSet());
            traversal.inE(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).
                    has(Schema.EdgeProperty.ROLE_LABEL_ID.name(), P.within(roleTypesIds)).outV();
        }

        return traversal.toStream().map(vertex -> vertex().tx().<Relationship>buildConcept(vertex));
    }

    private Stream<Relationship> edgeRelations(Role... roles){
        Set<Role> roleSet = new HashSet<>(Arrays.asList(roles));
        Stream<EdgeElement> stream = vertex().getEdgesOfType(Direction.BOTH, Schema.EdgeLabel.ATTRIBUTE);

        if(!roleSet.isEmpty()){
            stream = stream.filter(edge -> {
                Role roleOwner = vertex().tx().getSchemaConcept(LabelId.of(edge.property(Schema.EdgeProperty.RELATIONSHIP_ROLE_OWNER_LABEL_ID)));
                return roleSet.contains(roleOwner);
            });
        }

        return stream.map(edge -> vertex().tx().factory().buildRelation(edge));
    }

    @Override
    public Stream<Role> plays() {
        return castingsInstance().map(Casting::getRole);
    }

    @Override
    public T attribute(Attribute attribute) {
        attributeRelationship(attribute);
        return getThis();
    }

    public T attributeInferred(Attribute attribute) {
        attributeRelationship(attribute, true);
        return getThis();
    }

    @Override
    public Relationship attributeRelationship(Attribute attribute) {
        return attributeRelationship(attribute, false);
    }

    private Relationship attributeRelationship(Attribute attribute, boolean isInferred) {
        Schema.ImplicitType has = Schema.ImplicitType.HAS;
        Schema.ImplicitType hasValue = Schema.ImplicitType.HAS_VALUE;
        Schema.ImplicitType hasOwner  = Schema.ImplicitType.HAS_OWNER;

        //Is this attribute a key to me?
        if(type().keys().anyMatch(rt -> rt.equals(attribute.type()))){
            has = Schema.ImplicitType.KEY;
            hasValue = Schema.ImplicitType.KEY_VALUE;
            hasOwner  = Schema.ImplicitType.KEY_OWNER;
        }

        Label label = attribute.type().getLabel();
        RelationshipType hasAttribute = vertex().tx().getSchemaConcept(has.getLabel(label));
        Role hasAttributeOwner = vertex().tx().getSchemaConcept(hasOwner.getLabel(label));
        Role hasAttributeValue = vertex().tx().getSchemaConcept(hasValue.getLabel(label));

        if(hasAttribute == null || hasAttributeOwner == null || hasAttributeValue == null || type().plays().noneMatch(play -> play.equals(hasAttributeOwner))){
            throw GraknTxOperationException.hasNotAllowed(this, attribute);
        }

        EdgeElement attributeEdge = addEdge(AttributeImpl.from(attribute), Schema.EdgeLabel.ATTRIBUTE);
        if(isInferred) attributeEdge.property(Schema.EdgeProperty.IS_INFERRED, true);
        return vertex().tx().factory().buildRelation(attributeEdge, hasAttribute, hasAttributeOwner, hasAttributeValue);
    }

    @Override
    public T deleteAttribute(Attribute attribute){
        Role roleHasOwner = vertex().tx().getSchemaConcept(Schema.ImplicitType.HAS_OWNER.getLabel(attribute.type().getLabel()));
        Role roleKeyOwner = vertex().tx().getSchemaConcept(Schema.ImplicitType.KEY_OWNER.getLabel(attribute.type().getLabel()));

        Role roleHasValue = vertex().tx().getSchemaConcept(Schema.ImplicitType.HAS_VALUE.getLabel(attribute.type().getLabel()));
        Role roleKeyValue = vertex().tx().getSchemaConcept(Schema.ImplicitType.KEY_VALUE.getLabel(attribute.type().getLabel()));

        Stream<Relationship> relationships = relationships(filterNulls(roleHasOwner, roleKeyOwner));
        relationships.filter(relationship -> {
            Stream<Thing> rolePlayers = relationship.rolePlayers(filterNulls(roleHasValue, roleKeyValue));
            return rolePlayers.anyMatch(rolePlayer -> rolePlayer.equals(attribute));
        }).forEach(Concept::delete);

        return getThis();
    }

    /**
     * Returns an array with all the nulls filtered out.
     */
    private Role[] filterNulls(Role ... roles){
        return Arrays.stream(roles).filter(Objects::nonNull).toArray(Role[]::new);
    }

    /**
     *
     * @return The type of the concept casted to the correct interface
     */
    public V type() {
        return cachedType.get();
    }

    /**
     *
     * @param type The type of this concept
     */
    private void type(TypeImpl type) {
        if(type != null){
            //noinspection unchecked
            cachedType.set((V) type); //We cache the type early because it turns out we use it EVERY time. So this prevents many db reads
            type.currentShard().link(this);
            setInternalType(type);
        }
    }

    /**
     *
     * @param type The type of this concept
     */
    private void setInternalType(Type type){
        cachedInternalType.set(type.getLabel());
        vertex().property(Schema.VertexProperty.THING_TYPE_LABEL_ID, type.getLabelId().getValue());
    }

    /**
     *
     * @return The id of the type of this concept. This is a shortcut used to prevent traversals.
     */
    Label getInternalType(){
        return cachedInternalType.get();
    }

}
