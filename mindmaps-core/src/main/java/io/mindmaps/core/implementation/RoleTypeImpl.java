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

import io.mindmaps.core.model.*;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

class RoleTypeImpl extends TypeImpl<RoleType, Instance> implements RoleType{
    RoleTypeImpl(Vertex v, MindmapsTransactionImpl mindmapsGraph) {
        super(v, mindmapsGraph);
    }

    @Override
    public RelationType relationType() {
        Concept concept = getIncomingNeighbour(DataType.EdgeLabel.HAS_ROLE);

        if(concept == null){
            return null;
        } else {
            return getMindmapsTransaction().getElementFactory().buildRelationType(concept);
        }
    }

    @Override
    public Collection<Type> playedByTypes() {
        Collection<Type> types = new HashSet<>();
        getIncomingNeighbours(DataType.EdgeLabel.PLAYS_ROLE).forEach(c -> types.add(getMindmapsTransaction().getElementFactory().buildSpecificConceptType(c)));
        return types;
    }

    @Override
    public Collection<Instance> instances(){
        Set<Instance> instances = new HashSet<>();
        getIncomingNeighbours(DataType.EdgeLabel.ISA).forEach(concept -> {
            CastingImpl casting = (CastingImpl) concept;
            instances.add(casting.getRolePlayer());
        });
        return instances;
    }

    public Set<CastingImpl> castings(){
        Set<CastingImpl> castings = new HashSet<>();
        getIncomingNeighbours(DataType.EdgeLabel.ISA).forEach(concept -> {
            ((CastingImpl) concept).getRelations().forEach(relation -> mindmapsTransaction.getTransaction().putConcept(relation));
        });
        return castings;
    }
}
