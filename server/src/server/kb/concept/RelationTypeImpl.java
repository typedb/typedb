/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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
import grakn.core.server.kb.Cache;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.structure.Casting;
import grakn.core.server.kb.structure.EdgeElement;
import grakn.core.server.kb.structure.VertexElement;
import grakn.core.server.session.ConceptObserver;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An ontological element which categorises how instances may relate to each other.
 * A relation type defines how Type may relate to one another.
 * They are used to model and categorise n-ary relations.
 */
public class RelationTypeImpl extends TypeImpl<RelationType, Relation> implements RelationType {
    private final Cache<Set<Role>> cachedRelates = new Cache<>(() -> this.<Role>neighbours(Direction.OUT, Schema.EdgeLabel.RELATES).collect(Collectors.toSet()));

    RelationTypeImpl(VertexElement vertexElement, ConceptManager conceptBuilder, ConceptObserver conceptObserver) {
        super(vertexElement, conceptBuilder, conceptObserver);
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

    private Relation addRelation(boolean isInferred) {
        return conceptManager.createRelation(this, isInferred);
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
        cachedRelates.ifCached(set -> set.add(role));

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
        deleteEdge(Direction.OUT, Schema.EdgeLabel.RELATES, role);

        RoleImpl roleTypeImpl = (RoleImpl) role;

        // pass relation type, role, and castings to observer for validation
        List<Casting> conceptsPlayingRole = roleTypeImpl.rolePlayers().collect(Collectors.toList());
        conceptObserver.relationRoleUnrelated(this, role, conceptsPlayingRole);

        //Remove from internal cache
        cachedRelates.ifCached(set -> set.remove(role));

        //Remove from roleTypeCache
        ((RoleImpl) role).deleteCachedRelationType(this);

        return this;
    }

    @Override
    public void delete() {
        cachedRelates.get().forEach(r -> {
            RoleImpl role = ((RoleImpl) r);
            conceptObserver.roleDeleted(role);
            ((RoleImpl) r).deleteCachedRelationType(this);
        });

        super.delete();
    }

    @Override
    void trackRolePlayers() {
        conceptObserver.trackRelationInstancesRolePlayers(this);
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
        return roles()
                .flatMap(Role::players)
                .flatMap(type -> {
                    //Traversal is used here to take advantage of vertex centric index
                    // we use this more complex traversal to get to the instances of the Types that can
                    // play a role of this relation type
                    // from there we can access the edges that represent non-reified Concepts
                    // currently only Attribute can be non-reified

                    Stream<EdgeElement> edgeRelationsConnectedToTypeInstances = ConceptVertex.from(type).vertex()
                            .edgeRelationsConnectedToInstancesOfType(labelId());

                    return edgeRelationsConnectedToTypeInstances.map(edgeElement ->  conceptManager.buildConcept(edgeElement));
                });
    }
}
