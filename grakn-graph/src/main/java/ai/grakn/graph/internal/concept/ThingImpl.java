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

import ai.grakn.concept.Attribute;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.graph.internal.cache.Cache;
import ai.grakn.graph.internal.cache.Cacheable;
import ai.grakn.graph.internal.structure.Casting;
import ai.grakn.graph.internal.structure.EdgeElement;
import ai.grakn.graph.internal.structure.VertexElement;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.HashSet;
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
    private final Cache<Label> cachedInternalType = new Cache<>(Cacheable.label(), () -> {
        int typeId = vertex().property(Schema.VertexProperty.THING_TYPE_LABEL_ID);
        Type type = vertex().graph().getConcept(Schema.VertexProperty.LABEL_ID, typeId);
        return type.getLabel();
    });

    private final Cache<V> cachedType = new Cache<>(Cacheable.concept(), () -> {
        Optional<EdgeElement> typeEdge = vertex().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.ISA).
                flatMap(edge -> edge.target().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.SHARD)).findAny();

        if(!typeEdge.isPresent()) {
            throw GraphOperationException.noType(this);
        }

        return vertex().graph().factory().buildConcept(typeEdge.get().target());
    });

    ThingImpl(VertexElement vertexElement) {
        super(vertexElement);
    }

    ThingImpl(VertexElement vertexElement, V type) {
        this(vertexElement);
        type((TypeImpl) type);
    }

    /**
     * Deletes the concept as an Thing
     */
    @Override
    public void delete() {
        Set<Relationship> relationships = castingsInstance().map(Casting::getRelation).collect(Collectors.toSet());

        vertex().graph().txCache().removedInstance(type().getId());
        deleteNode();

        relationships.forEach(relation -> {
            if(relation.type().isImplicit()){//For now implicit relationships die
                relation.delete();
            } else {
                RelationshipImpl rel = (RelationshipImpl) relation;
                vertex().graph().txCache().trackForValidation(rel);
                rel.cleanUp();
            }
        });
    }

    @Override
    public void txCacheClear(){
        //TODO: Clearing the caches at th Thing Level may not be needed. Need to experiment
        cachedInternalType.clear();
        cachedType.clear();
    }

    /**
     * This index is used by concepts such as casting and relations to speed up internal lookups
     * @return The inner index value of some concepts.
     */
    public String getIndex(){
        return vertex().property(Schema.VertexProperty.INDEX);
    }

    /**
     *
     * @return All the {@link Attribute} that this Thing is linked with
     */
    public Stream<Attribute<?>> attributes(AttributeType... attributeTypes) {
        Set<ConceptId> resourceTypesIds = Arrays.stream(attributeTypes).map(Concept::getId).collect(Collectors.toSet());
        return resources(getShortcutNeighbours(), resourceTypesIds);
    }

    /**
     * Helper class which filters a {@link Stream} of {@link Attribute} to those of a specific set of {@link AttributeType}.
     *
     * @param conceptStream The {@link Stream} to filter
     * @param resourceTypesIds The {@link AttributeType} {@link ConceptId}s to filter to.
     * @return the filtered stream
     */
    private <X extends Concept> Stream<Attribute<?>> resources(Stream<X> conceptStream, Set<ConceptId> resourceTypesIds){
        Stream<Attribute<?>> resourceStream = conceptStream.
                filter(concept -> concept.isAttribute() && !this.equals(concept)).
                map(Concept::asAttribute);

        if(!resourceTypesIds.isEmpty()){
            resourceStream = resourceStream.filter(resource -> resourceTypesIds.contains(resource.type().getId()));
        }

        return resourceStream;
    }

    /**
     * Castings are retrieved from the perspective of the {@link Thing} which is a role player in a {@link Relationship}
     *
     * @return All the {@link Casting} which this instance is cast into the role
     */
    Stream<Casting> castingsInstance(){
        return vertex().getEdgesOfType(Direction.IN, Schema.EdgeLabel.SHORTCUT).
                map(edge -> vertex().graph().factory().buildCasting(edge));
    }

    <X extends Thing> Stream<X> getShortcutNeighbours(){
        GraphTraversal<Object, Vertex> shortcutTraversal = __.inE(Schema.EdgeLabel.SHORTCUT.getLabel()).
                as("edge").
                outV().
                outE(Schema.EdgeLabel.SHORTCUT.getLabel()).
                where(P.neq("edge")).
                inV();

        GraphTraversal<Object, Vertex> resourceEdgeTraversal = __.outE(Schema.EdgeLabel.RESOURCE.getLabel()).inV();

        //noinspection unchecked
        return vertex().graph().getTinkerTraversal().V().
                has(Schema.VertexProperty.ID.name(), getId().getValue()).
                union(shortcutTraversal, resourceEdgeTraversal).toStream().
                map(vertex -> vertex().graph().buildConcept(vertex));
    }

    /**
     *
     * @param roles An optional parameter which allows you to specify the role of the relations you wish to retrieve.
     * @return A set of Relations which the concept instance takes part in, optionally constrained by the Role Type.
     */
    @Override
    public Stream<Relationship> relations(Role... roles) {
        return Stream.concat(reifiedRelations(roles), edgeRelations(roles));
    }

    private Stream<Relationship> reifiedRelations(Role... roles){
        GraphTraversal<Vertex, Vertex> traversal = vertex().graph().getTinkerTraversal().V().
                has(Schema.VertexProperty.ID.name(), getId().getValue());

        if(roles.length == 0){
            traversal.in(Schema.EdgeLabel.SHORTCUT.getLabel());
        } else {
            Set<Integer> roleTypesIds = Arrays.stream(roles).map(r -> r.getLabelId().getValue()).collect(Collectors.toSet());
            traversal.inE(Schema.EdgeLabel.SHORTCUT.getLabel()).
                    has(Schema.EdgeProperty.ROLE_LABEL_ID.name(), P.within(roleTypesIds)).outV();
        }

        return traversal.toStream().map(vertex -> vertex().graph().buildConcept(vertex));
    }

    private Stream<Relationship> edgeRelations(Role... roles){
        Set<Role> roleSet = new HashSet<>(Arrays.asList(roles));
        Stream<EdgeElement> stream = vertex().getEdgesOfType(Direction.BOTH, Schema.EdgeLabel.RESOURCE);

        if(!roleSet.isEmpty()){
            stream = stream.filter(edge -> {
                Role roleOwner = vertex().graph().getOntologyConcept(LabelId.of(edge.property(Schema.EdgeProperty.RELATIONSHIP_ROLE_OWNER_LABEL_ID)));
                return roleSet.contains(roleOwner);
            });
        }

        return stream.map(edge -> vertex().graph().factory().buildRelation(edge));
    }

    @Override
    public Stream<Role> plays() {
        return castingsInstance().map(Casting::getRoleType);
    }

    @Override
    public T attribute(Attribute attribute){
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
        RelationshipType hasResource = vertex().graph().getSchemaConcept(has.getLabel(label));
        Role hasResourceOwner = vertex().graph().getSchemaConcept(hasOwner.getLabel(label));
        Role hasResourceValue = vertex().graph().getSchemaConcept(hasValue.getLabel(label));

        if(hasResource == null || hasResourceOwner == null || hasResourceValue == null || type().plays().noneMatch(play -> play.equals(hasResourceOwner))){
            throw GraphOperationException.hasNotAllowed(this, attribute);
        }

        EdgeElement resourceEdge = putEdge(AttributeImpl.from(attribute), Schema.EdgeLabel.RESOURCE);
        vertex().graph().factory().buildRelation(resourceEdge, hasResource, hasResourceOwner, hasResourceValue);

        return getThis();
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
            type.currentShard().link(this);
            setInternalType(type());
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
