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
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
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
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.Collection;
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
 *     Instances can relate to one another via {@link Relation}
 * </p>
 *
 * @author fppt
 *
 * @param <T> The leaf interface of the object concept which extends {@link Thing}.
 *           For example {@link ai.grakn.concept.Entity} or {@link Relation}.
 * @param <V> The type of the concept which extends {@link Type} of the concept.
 *           For example {@link ai.grakn.concept.EntityType} or {@link RelationType}
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
        Set<Relation> relations = castingsInstance().map(Casting::getRelation).collect(Collectors.toSet());

        vertex().graph().txCache().removedInstance(type().getId());
        deleteNode();

        relations.forEach(relation -> {
            if(relation.type().isImplicit()){//For now implicit relations die
                relation.delete();
            } else {
                RelationImpl rel = (RelationImpl) relation;
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
     * @return All the {@link Resource} that this Thing is linked with
     */
    public Collection<Resource<?>> resources(ResourceType... resourceTypes) {
        Set<ConceptId> resourceTypesIds = Arrays.stream(resourceTypes).map(Concept::getId).collect(Collectors.toSet());

        Set<Resource<?>> resources = new HashSet<>();

        //Get Resources Attached Via Implicit Relations
        getShortcutNeighbours().forEach(concept -> {
            if(concept.isResource() && !equals(concept)) {
                Resource<?> resource = concept.asResource();
                if(resourceTypesIds.isEmpty() || resourceTypesIds.contains(resource.type().getId())) {
                    resources.add(resource);
                }
            }
        });

        //Get Resources Attached Via resource edge
        neighbours(Direction.OUT, Schema.EdgeLabel.RESOURCE).forEach(resource -> {
            if(resourceTypesIds.isEmpty() || resourceTypesIds.contains(resource.asThing().type().getId())) {
                resources.add(resource.asResource());
            }
        });

        return resources;
    }

    /**
     * Castings are retrieved from the perspective of the {@link Thing} which is a role player in a {@link Relation}
     *
     * @return All the {@link Casting} which this instance is cast into the role
     */
    Stream<Casting> castingsInstance(){
        return vertex().getEdgesOfType(Direction.IN, Schema.EdgeLabel.SHORTCUT).
                map(edge -> vertex().graph().factory().buildCasting(edge));
    }

    <X extends Thing> Set<X> getShortcutNeighbours(){
        Set<X> foundNeighbours = new HashSet<X>();
        vertex().graph().getTinkerTraversal().V().
                has(Schema.VertexProperty.ID.name(), getId().getValue()).
                inE(Schema.EdgeLabel.SHORTCUT.getLabel()).
                as("edge").
                outV().
                outE(Schema.EdgeLabel.SHORTCUT.getLabel()).
                where(P.neq("edge")).
                inV().
                forEachRemaining(vertex -> foundNeighbours.add(vertex().graph().buildConcept(vertex)));
        return foundNeighbours;
    }

    /**
     *
     * @param roles An optional parameter which allows you to specify the role of the relations you wish to retrieve.
     * @return A set of Relations which the concept instance takes part in, optionally constrained by the Role Type.
     */
    @Override
    public Collection<Relation> relations(Role... roles) {
        Set<Relation> relations = reifiedRelations(roles);
        relations.addAll(edgeRelations(roles));

        return relations;
    }

    private Set<Relation> reifiedRelations(Role... roles){
        Set<Relation> relations = new HashSet<>();
        GraphTraversal<Vertex, Vertex> traversal = vertex().graph().getTinkerTraversal().V().
                has(Schema.VertexProperty.ID.name(), getId().getValue());

        if(roles.length == 0){
            traversal.in(Schema.EdgeLabel.SHORTCUT.getLabel());
        } else {
            Set<Integer> roleTypesIds = Arrays.stream(roles).map(r -> r.getLabelId().getValue()).collect(Collectors.toSet());
            traversal.inE(Schema.EdgeLabel.SHORTCUT.getLabel()).
                    has(Schema.EdgeProperty.ROLE_LABEL_ID.name(), P.within(roleTypesIds)).outV();
        }
        traversal.forEachRemaining(v -> relations.add(vertex().graph().buildConcept(v)));

        return relations;
    }

    private Set<Relation> edgeRelations(Role... roles){
        Set<Role> roleSet = new HashSet<>(Arrays.asList(roles));
        Set<Relation> relations = new HashSet<>();

        vertex().getEdgesOfType(Direction.BOTH, Schema.EdgeLabel.RESOURCE).forEach(edge -> {
            if (roleSet.isEmpty()) {
                relations.add(vertex().graph().factory().buildRelation(edge));
            } else {
                Role roleOwner = vertex().graph().getOntologyConcept(LabelId.of(edge.property(Schema.EdgeProperty.RELATION_ROLE_OWNER_LABEL_ID)));
                if(roleSet.contains(roleOwner)){
                    relations.add(vertex().graph().factory().buildRelation(edge));
                }
            }
        });

        return relations;
    }

    /**
     *
     * @return A set of all the Role Types which this instance plays.
     */
    @Override
    public Collection<Role> plays() {
        return castingsInstance().map(Casting::getRoleType).collect(Collectors.toSet());
    }


    /**
     * Creates a relation from this instance to the provided resource.
     * @param resource The resource to creating a relationship to
     * @return The instance itself
     */
    @Override
    public T resource(Resource resource){
        Schema.ImplicitType has = Schema.ImplicitType.HAS;
        Schema.ImplicitType hasValue = Schema.ImplicitType.HAS_VALUE;
        Schema.ImplicitType hasOwner  = Schema.ImplicitType.HAS_OWNER;

        //Is this resource a key to me?
        if(type().keys().contains(resource.type())){
            has = Schema.ImplicitType.KEY;
            hasValue = Schema.ImplicitType.KEY_VALUE;
            hasOwner  = Schema.ImplicitType.KEY_OWNER;
        }


        Label label = resource.type().getLabel();
        RelationType hasResource = vertex().graph().getOntologyConcept(has.getLabel(label));
        Role hasResourceOwner = vertex().graph().getOntologyConcept(hasOwner.getLabel(label));
        Role hasResourceValue = vertex().graph().getOntologyConcept(hasValue.getLabel(label));

        if(hasResource == null || hasResourceOwner == null || hasResourceValue == null || !type().plays().contains(hasResourceOwner)){
            throw GraphOperationException.hasNotAllowed(this, resource);
        }

        EdgeElement resourceEdge = putEdge(ResourceImpl.from(resource), Schema.EdgeLabel.RESOURCE);
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
