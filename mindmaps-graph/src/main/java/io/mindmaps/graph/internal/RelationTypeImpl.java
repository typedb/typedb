/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graph.internal;

import io.mindmaps.util.Schema;
import io.mindmaps.concept.Relation;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.RoleType;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A Relation Type is an ontological element used to concept how entity types relate to one another.
 */
class RelationTypeImpl extends TypeImpl<RelationType, Relation> implements RelationType {
    RelationTypeImpl(Vertex v, AbstractMindmapsGraph mindmapsGraph) {
        super(v, mindmapsGraph);
    }

    /**
     *
     * @return A list of the Role Types which make up this Relation Type.
     */
    @Override
    public Collection<RoleType> hasRoles() {
        Set<RoleType> roleTypes = new HashSet<>();
        getOutgoingNeighbours(Schema.EdgeLabel.HAS_ROLE).forEach(role -> roleTypes.add(getMindmapsGraph().getElementFactory().buildRoleType(role)));
        return roleTypes;
    }

    /**
     *
     * @param roleType A new role which is part of this relationship.
     * @return The Relation Type itself.
     */
    @Override
    public RelationType hasRole(RoleType roleType) {
        putEdge(getMindmapsGraph().getElementFactory().buildRoleType(roleType), Schema.EdgeLabel.HAS_ROLE);
        return this;
    }

    /**
     *
     * @param roleType The role type to delete from this relationship.
     * @return The Relation Type itself.
     */
    @Override
    public RelationType deleteHasRole(RoleType roleType) {
        deleteEdgeTo(Schema.EdgeLabel.HAS_ROLE, getMindmapsGraph().getElementFactory().buildRoleType(roleType));
        //Add castings of roleType to make sure relations are still valid
        ((RoleTypeImpl) roleType).castings().forEach(casting -> mindmapsGraph.getConceptLog().putConcept(casting));
        return this;
    }
}
