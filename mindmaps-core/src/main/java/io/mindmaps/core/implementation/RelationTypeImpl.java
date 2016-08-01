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

package io.mindmaps.core.implementation;

import io.mindmaps.core.model.Relation;
import io.mindmaps.core.model.RelationType;
import io.mindmaps.core.model.RoleType;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

class RelationTypeImpl extends TypeImpl<RelationType, Relation> implements RelationType {
    RelationTypeImpl(Vertex v, MindmapsTransactionImpl mindmapsGraph) {
        super(v, mindmapsGraph);
    }

    @Override
    public Collection<RoleType> hasRoles() {
        Set<RoleType> roleTypes = new HashSet<>();
        getOutgoingNeighbours(DataType.EdgeLabel.HAS_ROLE).forEach(role -> roleTypes.add(getMindmapsTransaction().getElementFactory().buildRoleType(role)));
        return roleTypes;
    }

    @Override
    public RelationType hasRole(RoleType roleType) {
        putEdge(getMindmapsTransaction().getElementFactory().buildRoleType(roleType), DataType.EdgeLabel.HAS_ROLE);
        return this;
    }

    @Override
    public RelationType deleteHasRole(RoleType roleType) {
        deleteEdgeTo(DataType.EdgeLabel.HAS_ROLE, getMindmapsTransaction().getElementFactory().buildRoleType(roleType));
        //Add castings of roleType to make sure relations are still valid
        ((RoleTypeImpl) roleType).castings().forEach(casting -> mindmapsTransaction.getTransaction().putConcept(casting));
        return this;
    }
}
