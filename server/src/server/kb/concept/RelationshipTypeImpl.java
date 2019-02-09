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

import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.Relation;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Type;
import grakn.core.graql.internal.Schema;
import grakn.core.server.kb.cache.Cache;
import grakn.core.server.kb.cache.Cacheable;
import grakn.core.server.kb.structure.VertexElement;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <p>
 *     An ontological element which categorises how instances may relate to each other.
 * </p>
 *
 * <p>
 *     A relation type defines how {@link Type} may relate to one another.
 *     They are used to model and categorise n-ary relationships.
 * </p>
 *
 *
 */
public class RelationshipTypeImpl extends TypeImpl<RelationType, Relation> implements RelationType {
    private final Cache<Set<Role>> cachedRelates = Cache.createSessionCache(this, Cacheable.set(), () -> this.<Role>neighbours(Direction.OUT, Schema.EdgeLabel.RELATES).collect(Collectors.toSet()));

    private RelationshipTypeImpl(VertexElement vertexElement) {
        super(vertexElement);
    }

    private RelationshipTypeImpl(VertexElement vertexElement, RelationType type) {
        super(vertexElement, type);
    }

    public static RelationshipTypeImpl get(VertexElement vertexElement){
        return new RelationshipTypeImpl(vertexElement);
    }

    public static RelationshipTypeImpl create(VertexElement vertexElement, RelationType type){
        RelationshipTypeImpl relationType = new RelationshipTypeImpl(vertexElement, type);
        vertexElement.tx().cache().trackForValidation(relationType);
        return relationType;
    }

    @Override
    public Relation create() {
        return addRelationship(false);
    }

    public Relation addRelationshipInferred() {
        return addRelationship(true);
    }

    public Relation addRelationship(boolean isInferred) {
        Relation relationship = addInstance(Schema.BaseType.RELATIONSHIP,
                                            (vertex, type) -> vertex().tx().factory().buildRelation(vertex, type), isInferred);
        vertex().tx().cache().addNewRelationship(relationship);
        return relationship;
    }



    @Override
    public Stream<Role> roles() {
        return cachedRelates.get().stream();
    }

    @Override
    public RelationType relates(Role role) {
        checkSchemaMutationAllowed();
        putEdge(ConceptVertex.from(role), Schema.EdgeLabel.RELATES);

        //TODO: the following lines below this comment should only be executed if the edge is added

        //Cache the Role internally
        cachedRelates.ifPresent(set -> set.add(role));

        //Cache the relation type in the role
        ((RoleImpl) role).addCachedRelationType(this);

        return this;
    }

    /**
     *
     * @param role The {@link Role} to delete from this {@link RelationType}.
     * @return The {@link Relation} Type itself.
     */
    @Override
    public RelationType unrelate(Role role) {
        checkSchemaMutationAllowed();
        deleteEdge(Direction.OUT, Schema.EdgeLabel.RELATES, (Concept) role);

        RoleImpl roleTypeImpl = (RoleImpl) role;
        //Add roleplayers of role to make sure relations are still valid
        roleTypeImpl.rolePlayers().forEach(rolePlayer -> vertex().tx().cache().trackForValidation(rolePlayer));


        //Add the Role Type itself
        vertex().tx().cache().trackForValidation(roleTypeImpl);

        //Add the Relationship Type
        vertex().tx().cache().trackForValidation(this);

        //Remove from internal cache
        cachedRelates.ifPresent(set -> set.remove(role));

        //Remove from roleTypeCache
        ((RoleImpl) role).deleteCachedRelationType(this);

        return this;
    }

    @Override
    public void delete(){
        cachedRelates.get().forEach(r -> {
            RoleImpl role = ((RoleImpl) r);
            vertex().tx().cache().trackForValidation(role);
            ((RoleImpl) r).deleteCachedRelationType(this);
        });

        super.delete();
    }

    @Override
    void trackRolePlayers(){
        instances().forEach(concept -> {
            RelationshipImpl relation = RelationshipImpl.from(concept);
            if(relation.reified().isPresent()){
                relation.reified().get().castingsRelation().forEach(rolePlayer -> vertex().tx().cache().trackForValidation(rolePlayer));
            }
        });
    }

    @Override
    public Stream<Relation> instancesDirect(){
        Stream<Relation> instances = super.instancesDirect();

        //If the relation type is implicit then we need to get any relation edges it may have.
        if(isImplicit()) instances = Stream.concat(instances, relationEdges());

        return instances;
    }

    private Stream<Relation> relationEdges(){
        //Unfortunately this is a slow process
        return roles().
                flatMap(Role::players).
                flatMap(type ->{
                    //Traversal is used here to take advantage of vertex centric index
                    return  vertex().tx().getTinkerTraversal().V().
                            has(Schema.VertexProperty.ID.name(), type.id().getValue()).
                            in(Schema.EdgeLabel.SHARD.getLabel()).
                            in(Schema.EdgeLabel.ISA.getLabel()).
                            outE(Schema.EdgeLabel.ATTRIBUTE.getLabel()).
                            has(Schema.EdgeProperty.RELATIONSHIP_TYPE_LABEL_ID.name(), labelId().getValue()).
                            toStream().
                            map(edge -> vertex().tx().factory().<Relation>buildConcept(edge));
                });
    }

    public static RelationshipTypeImpl from(RelationType relationshipType){
        return (RelationshipTypeImpl) relationshipType;
    }
}
