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

package ai.grakn.graph.internal;

import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A Relation Type is an ontological element used to concept how entity types relate to one another.
 */
class RelationTypeImpl extends TypeImpl<RelationType, Relation> implements RelationType {
    RelationTypeImpl(Vertex v, Type type, Boolean isImplicit, AbstractGraknGraph graknGraph) {
        super(v, type, isImplicit, graknGraph);
    }
    RelationTypeImpl(Vertex v, Type type, AbstractGraknGraph graknGraph) {
        super(v, type, graknGraph);
    }

    @Override
    public Relation addRelation() {
        return addInstance(Schema.BaseType.RELATION, (vertex, type) -> {
            RelationImpl relation = getGraknGraph().getElementFactory().buildRelation(vertex, type);
            relation.setHash(null);
            return relation;
        });
    }

    /**
     *
     * @return A list of the Role Types which make up this Relation Type.
     */
    @Override
    public Collection<RoleType> hasRoles() {
        Set<RoleType> roleTypes = new HashSet<>();
        getOutgoingNeighbours(Schema.EdgeLabel.HAS_ROLE).forEach(role -> roleTypes.add(role.asRoleType()));
        return roleTypes;
    }

    /**
     *
     * @param roleType A new role which is part of this relationship.
     * @return The Relation Type itself.
     */
    @Override
    public RelationType hasRole(RoleType roleType) {
        checkTypeMutation();
        putEdge(roleType, Schema.EdgeLabel.HAS_ROLE);
        return this;
    }

    /**
     *
     * @param roleType The role type to delete from this relationship.
     * @return The Relation Type itself.
     */
    @Override
    public RelationType deleteHasRole(RoleType roleType) {
        checkTypeMutation();
        deleteEdgeTo(Schema.EdgeLabel.HAS_ROLE, roleType);
        //Add castings of roleType to make sure relations are still valid
        ((RoleTypeImpl) roleType).castings().forEach(casting -> getGraknGraph().getConceptLog().putConcept(casting));
        return this;
    }
}
