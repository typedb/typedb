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

import com.google.common.collect.Sets;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.LabelId;
import grakn.core.graql.concept.Relation;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.concept.Thing;
import grakn.core.graql.concept.Type;
import grakn.core.graql.internal.Schema;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.kb.cache.Cache;
import grakn.core.server.kb.cache.Cacheable;
import grakn.core.server.kb.structure.Casting;
import grakn.core.server.kb.structure.EdgeElement;
import grakn.core.server.kb.structure.VertexElement;
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
import java.util.stream.Stream;

import static grakn.core.graql.internal.Schema.EdgeProperty.ROLE_LABEL_ID;
import static java.util.stream.Collectors.toSet;

/**
 * <p>
 *     A data instance in the graph belonging to a specific Type
 * </p>
 *
 * <p>
 *     Instances represent data in the graph.
 *     Every instance belongs to a Type which serves as a way of categorising them.
 *     Instances can relate to one another via Relation
 * </p>
 *
 *
 * @param <T> The leaf interface of the object concept which extends Thing.
 *           For example Entity or Relation.
 * @param <V> The type of the concept which extends Type of the concept.
 *           For example EntityType or RelationType
 */
public abstract class ThingImpl<T extends Thing, V extends Type> extends ConceptImpl implements Thing {
    private final Cache<Label> cachedInternalType = Cache.createTxCache(this, Cacheable.label(), () -> {
        int typeId = vertex().property(Schema.VertexProperty.THING_TYPE_LABEL_ID);
        Optional<Type> type = vertex().tx().getConcept(Schema.VertexProperty.LABEL_ID, typeId);
        return type.orElseThrow(() -> TransactionException.missingType(id())).label();
    });

    private final Cache<V> cachedType = Cache.createTxCache(this, Cacheable.concept(), () -> {
        Optional<V> type = vertex().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.ISA).
                map(EdgeElement::target).
                flatMap(edge -> edge.getEdgesOfType(Direction.OUT, Schema.EdgeLabel.SHARD)).
                map(EdgeElement::target).
                map(concept -> vertex().tx().factory().<V>buildConcept(concept)).
                findAny();

        return type.orElseThrow(() -> TransactionException.noType(this));
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
     * This Thing gets tracked for validation only if it has keys which need to be checked.
     */
    private void track(){
        if(type().keys().findAny().isPresent()){
            vertex().tx().cache().trackForValidation(this);
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
        Set<Relation> relationships = castingsInstance().map(casting -> {
            Relation relationship = casting.getRelationship();
            Role role = casting.getRole();
            relationship.unassign(role, this);
            return relationship;
        }).collect(toSet());

        vertex().tx().cache().removedInstance(type().id());
        deleteNode();

        relationships.forEach(relation -> {
            if(relation.type().isImplicit()){//For now implicit relationships die
                relation.delete();
            } else {
                RelationImpl rel = (RelationImpl) relation;
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
        Set<ConceptId> attributeTypesIds = Arrays.stream(attributeTypes).map(Concept::id).collect(toSet());
        return attributes(getShortcutNeighbours(true), attributeTypesIds);
    }

    @Override
    public Stream<Attribute<?>> keys(AttributeType... attributeTypes){
        Set<ConceptId> attributeTypesIds = Arrays.stream(attributeTypes).map(Concept::id).collect(toSet());
        Set<ConceptId> keyTypeIds = type().keys().map(Concept::id).collect(toSet());

        if(!attributeTypesIds.isEmpty()){
            keyTypeIds = Sets.intersection(attributeTypesIds, keyTypeIds);
        }

        if(keyTypeIds.isEmpty()) return Stream.empty();

        return attributes(getShortcutNeighbours(true), keyTypeIds);
    }

    /**
     * Helper class which filters a Stream of Attribute to those of a specific set of AttributeType.
     *
     * @param conceptStream The Stream to filter
     * @param attributeTypesIds The AttributeType ConceptIds to filter to.
     * @return the filtered stream
     */
    private <X extends Concept> Stream<Attribute<?>> attributes(Stream<X> conceptStream, Set<ConceptId> attributeTypesIds){
        Stream<Attribute<?>> attributeStream = conceptStream.
                filter(concept -> concept.isAttribute() && !this.equals(concept)).
                map(Concept::asAttribute);

        if(!attributeTypesIds.isEmpty()){
            attributeStream = attributeStream.filter(attribute -> attributeTypesIds.contains(attribute.type().id()));
        }

        return attributeStream;
    }

    /**
     * Castings are retrieved from the perspective of the Thing which is a role player in a Relation
     *
     * @return All the Casting which this instance is cast into the role
     */
    Stream<Casting> castingsInstance(){
        return vertex().getEdgesOfType(Direction.IN, Schema.EdgeLabel.ROLE_PLAYER).
                map(edge -> Casting.withThing(edge, this));
    }

    private Set<Integer> implicitLabelsToIds(Set<Label> labels, Set<Schema.ImplicitType> implicitTypes){
        return labels.stream()
                .flatMap(label -> implicitTypes.stream().map(it -> it.getLabel(label)))
                .map(label -> vertex().tx().convertToId(label))
                .filter(id -> !id.equals(LabelId.invalid()))
                .map(LabelId::getValue)
                .collect(toSet());
    }

    <X extends Thing> Stream<X> getShortcutNeighbours(boolean ownerToValueOrdering, AttributeType... attributeTypes){
        Set<AttributeType> completeAttributeTypes = new HashSet<>(Arrays.asList(attributeTypes));
        if (completeAttributeTypes.isEmpty()) completeAttributeTypes.add(vertex().tx().getMetaAttributeType());

        Set<Label> attributeHierachyLabels = completeAttributeTypes.stream()
                .flatMap(t -> (Stream<AttributeType>) t.subs())
                .map(SchemaConcept::label)
                .collect(toSet());

        Set<Integer> ownerRoleIds = implicitLabelsToIds(
                attributeHierachyLabels,
                Sets.newHashSet(
                        Schema.ImplicitType.HAS_OWNER,
                        Schema.ImplicitType.KEY_OWNER
                ));
        Set<Integer> valueRoleIds = implicitLabelsToIds(
                attributeHierachyLabels,
                Sets.newHashSet(
                        Schema.ImplicitType.HAS_VALUE,
                        Schema.ImplicitType.KEY_VALUE
                ));

        //NB: need extra check cause it seems valid types can still produce invalid ids
        GraphTraversal<Vertex, Vertex> shortcutTraversal = !(ownerRoleIds.isEmpty() || valueRoleIds.isEmpty())?
                __.inE(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).
                        as("edge").
                        has(ROLE_LABEL_ID.name(), P.within(ownerToValueOrdering? ownerRoleIds : valueRoleIds)).
                        outV().
                        outE(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).
                        has(ROLE_LABEL_ID.name(), P.within(ownerToValueOrdering? valueRoleIds : ownerRoleIds)).
                        where(P.neq("edge")).
                        inV()
                :
                __.inE(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).
                        as("edge").
                        outV().
                        outE(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).
                        where(P.neq("edge")).
                        inV();

        GraphTraversal<Vertex, Vertex> attributeEdgeTraversal = __.outE(Schema.EdgeLabel.ATTRIBUTE.getLabel()).inV();

        //noinspection unchecked
        return vertex().tx().getTinkerTraversal().V().
                has(Schema.VertexProperty.ID.name(), id().getValue()).
                union(shortcutTraversal, attributeEdgeTraversal).toStream().
                map(vertex -> vertex().tx().<X>buildConcept(vertex));
    }

    /**
     *
     * @param roles An optional parameter which allows you to specify the role of the relations you wish to retrieve.
     * @return A set of Relations which the concept instance takes part in, optionally constrained by the Role Type.
     */
    @Override
    public Stream<Relation> relationships(Role... roles) {
        return Stream.concat(reifiedRelations(roles), edgeRelations(roles));
    }

    private Stream<Relation> reifiedRelations(Role... roles){
        GraphTraversal<Vertex, Vertex> traversal = vertex().tx().getTinkerTraversal().V().
                has(Schema.VertexProperty.ID.name(), id().getValue());

        if(roles.length == 0){
            traversal.in(Schema.EdgeLabel.ROLE_PLAYER.getLabel());
        } else {
            Set<Integer> roleTypesIds = Arrays.stream(roles).map(r -> r.labelId().getValue()).collect(toSet());
            traversal.inE(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).
                    has(Schema.EdgeProperty.ROLE_LABEL_ID.name(), P.within(roleTypesIds)).outV();
        }

        return traversal.toStream().map(vertex -> vertex().tx().<Relation>buildConcept(vertex));
    }

    private Stream<Relation> edgeRelations(Role... roles){
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
    public Stream<Role> roles() {
        return castingsInstance().map(Casting::getRole);
    }

    @Override
    public T has(Attribute attribute) {
        relhas(attribute);
        return getThis();
    }

    public T attributeInferred(Attribute attribute) {
        attributeRelationship(attribute, true);
        return getThis();
    }

    @Override
    public Relation relhas(Attribute attribute) {
        return attributeRelationship(attribute, false);
    }

    private Relation attributeRelationship(Attribute attribute, boolean isInferred) {
        Schema.ImplicitType has = Schema.ImplicitType.HAS;
        Schema.ImplicitType hasValue = Schema.ImplicitType.HAS_VALUE;
        Schema.ImplicitType hasOwner  = Schema.ImplicitType.HAS_OWNER;

        //Is this attribute a key to me?
        if(type().keys().anyMatch(rt -> rt.equals(attribute.type()))){
            has = Schema.ImplicitType.KEY;
            hasValue = Schema.ImplicitType.KEY_VALUE;
            hasOwner  = Schema.ImplicitType.KEY_OWNER;
        }

        Label label = attribute.type().label();
        RelationType hasAttribute = vertex().tx().getSchemaConcept(has.getLabel(label));
        Role hasAttributeOwner = vertex().tx().getSchemaConcept(hasOwner.getLabel(label));
        Role hasAttributeValue = vertex().tx().getSchemaConcept(hasValue.getLabel(label));

        if(hasAttribute == null || hasAttributeOwner == null || hasAttributeValue == null || type().playing().noneMatch(play -> play.equals(hasAttributeOwner))){
            throw TransactionException.hasNotAllowed(this, attribute);
        }

        EdgeElement attributeEdge = addEdge(AttributeImpl.from(attribute), Schema.EdgeLabel.ATTRIBUTE);
        if(isInferred) attributeEdge.property(Schema.EdgeProperty.IS_INFERRED, true);
        return vertex().tx().factory().buildRelation(attributeEdge, hasAttribute, hasAttributeOwner, hasAttributeValue);
    }

    @Override
    public T unhas(Attribute attribute){
        Role roleHasOwner = vertex().tx().getSchemaConcept(Schema.ImplicitType.HAS_OWNER.getLabel(attribute.type().label()));
        Role roleKeyOwner = vertex().tx().getSchemaConcept(Schema.ImplicitType.KEY_OWNER.getLabel(attribute.type().label()));

        Role roleHasValue = vertex().tx().getSchemaConcept(Schema.ImplicitType.HAS_VALUE.getLabel(attribute.type().label()));
        Role roleKeyValue = vertex().tx().getSchemaConcept(Schema.ImplicitType.KEY_VALUE.getLabel(attribute.type().label()));

        Stream<Relation> relationships = relationships(filterNulls(roleHasOwner, roleKeyOwner));
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
        cachedInternalType.set(type.label());
        vertex().property(Schema.VertexProperty.THING_TYPE_LABEL_ID, type.labelId().getValue());
    }

    /**
     *
     * @return The id of the type of this concept. This is a shortcut used to prevent traversals.
     */
    Label getInternalType(){
        return cachedInternalType.get();
    }

}
