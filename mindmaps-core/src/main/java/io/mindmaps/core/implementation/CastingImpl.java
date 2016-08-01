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

import io.mindmaps.core.exceptions.NoEdgeException;
import io.mindmaps.core.model.*;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

class CastingImpl extends ConceptImpl {

    CastingImpl(Vertex v, MindmapsTransactionImpl mindmapsGraph) {
        super(v, mindmapsGraph);
    }

    public RoleTypeImpl getRole() {
        Concept concept = getParentIsa();
        if(concept != null)
            return getMindmapsTransaction().getElementFactory().buildRoleType(concept);
        else
            throw new NoEdgeException(toString(), DataType.BaseType.ROLE_TYPE.name());
    }

    public InstanceImpl getRolePlayer() {
        Concept concept = getOutgoingNeighbour(DataType.EdgeLabel.ROLE_PLAYER);
        if(concept != null)
            return getMindmapsTransaction().getElementFactory().buildSpecificInstance(concept);
        else
            return null;
    }

    public CastingImpl setHash(RoleTypeImpl role, InstanceImpl rolePlayer){
        String hash;
        if(getMindmapsTransaction().isBatchLoadingEnabled())
            hash = "CastingBaseId_" + this.getBaseIdentifier() + UUID.randomUUID().toString();
        else
            hash = generateNewHash(role, rolePlayer);
        setUniqueProperty(DataType.ConceptPropertyUnique.INDEX, hash);
        return this;
    }

    public static String generateNewHash(RoleTypeImpl role, InstanceImpl rolePlayer){
        return "Casting-Role-" + role.getId() + "-RolePlayer-" + rolePlayer.getId();
    }

    public Set<RelationImpl> getRelations() {
        ConceptImpl<?, ?, ?> thisRef = this;
        Set<RelationImpl> relations = new HashSet<>();
        Set<ConceptImpl> concepts = thisRef.getIncomingNeighbours(DataType.EdgeLabel.CASTING);

        if(concepts.size() > 0){
            relations.addAll(concepts.stream().map(getMindmapsTransaction().getElementFactory()::buildRelation).collect(Collectors.toList()));
        } else {
            throw new NoEdgeException(toString(), DataType.BaseType.RELATION.name());
        }

        return relations;
    }
}
