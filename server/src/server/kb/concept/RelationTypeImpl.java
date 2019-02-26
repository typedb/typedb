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

import grakn.core.concept.Concept;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.cache.Cache;
import grakn.core.server.kb.cache.Cacheable;
import grakn.core.server.kb.structure.VertexElement;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An ontological element which categorises how instances may relate to each other.
 * A relation type defines how Type may relate to one another.
 * They are used to model and categorise n-ary relations.
 */
public class RelationTypeImpl extends TypeImpl<RelationType, Relation> implements RelationType {
    private final Cache<Set<Role>> cachedRelates = Cache.createSessionCache(this, Cacheable.set(), () -> this.<Role>neighbours(Direction.OUT, Schema.EdgeLabel.RELATES).collect(Collectors.toSet()));

    private RelationTypeImpl(VertexElement vertexElement) {
        super(vertexElement);
    }

    private RelationTypeImpl(VertexElement vertexElement, RelationType type) {
        super(vertexElement, type);
    }

    public static RelationTypeImpl get(VertexElement vertexElement) {
        return new RelationTypeImpl(vertexElement);
    }

    public static RelationTypeImpl create(VertexElement vertexElement, RelationType type) {
        RelationTypeImpl relationType = new RelationTypeImpl(vertexElement, type);
        vertexElement.tx().cache().trackForValidation(relationType);
        return relationType;
    }

    public static RelationTypeImpl from(RelationType relationType) {
        return (RelationTypeImpl) relationType;
    }

    @Override
    public Relation create() {
        return addRelation(false);
    }

    public Relation addRelationInferred() {
        return addRelation(true);
    }

    public Relation addRelation(boolean isInferred) {
        Relation relation = addInstance(Schema.BaseType.RELATION,
                                            (vertex, type) -> vertex().tx().factory().buildRelation(vertex, type), isInferred);
        vertex().tx().cache().addNewRelation(relation);
        return relation;
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
     * @param role The Role to delete from this RelationType.
     * @return The Relation Type itself.
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

        //Add the Relation Type
        vertex().tx().cache().trackForValidation(this);

        //Remove from internal cache
        cachedRelates.ifPresent(set -> set.remove(role));

        //Remove from roleTypeCache
        ((RoleImpl) role).deleteCachedRelationType(this);

        return this;
    }

    @Override
    public void delete() {
        cachedRelates.get().forEach(r -> {
            RoleImpl role = ((RoleImpl) r);
            vertex().tx().cache().trackForValidation(role);
            ((RoleImpl) r).deleteCachedRelationType(this);
        });

        super.delete();
    }

    @Override
    void trackRolePlayers() {
        instances().forEach(concept -> {
            RelationImpl relation = RelationImpl.from(concept);
            if (relation.reified().isPresent()) {
                relation.reified().get().castingsRelation().forEach(rolePlayer -> vertex().tx().cache().trackForValidation(rolePlayer));
            }
        });
    }

    @Override
    public Stream<Relation> instancesDirect() {
        Stream<Relation> instances = super.instancesDirect();

        //If the relation type is implicit then we need to get any relation edges it may have.
        if (isImplicit()) instances = Stream.concat(instances, relationEdges());

        return instances;
    }

    private Stream<Relation> relationEdges() {
        //Unfortunately this is a slow process
        return roles().
                flatMap(Role::players).
                flatMap(type -> {
                    //Traversal is used here to take advantage of vertex centric index
                    return vertex().tx().getTinkerTraversal().V().
                            has(Schema.VertexProperty.ID.name(), type.id().getValue()).
                            in(Schema.EdgeLabel.SHARD.getLabel()).
                            in(Schema.EdgeLabel.ISA.getLabel()).
                            outE(Schema.EdgeLabel.ATTRIBUTE.getLabel()).
                            has(Schema.EdgeProperty.RELATION_TYPE_LABEL_ID.name(), labelId().getValue()).
                            toStream().
                            map(edge -> vertex().tx().factory().buildConcept(edge));
                });
    }
}
