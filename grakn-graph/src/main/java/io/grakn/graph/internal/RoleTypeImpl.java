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

package io.grakn.graph.internal;

import io.grakn.concept.Concept;
import io.grakn.concept.Instance;
import io.grakn.concept.RelationType;
import io.grakn.concept.RoleType;
import io.grakn.concept.Type;
import io.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * An ontological element which defines a role which can be played in a relation type.
 */
class RoleTypeImpl extends TypeImpl<RoleType, Instance> implements RoleType{
    RoleTypeImpl(Vertex v, Type type, AbstractGraknGraph mindmapsGraph) {
        super(v, type, mindmapsGraph);
    }

    /**
     *
     * @return The Relation Type which this role takes part in.
     */
    @Override
    public RelationType relationType() {
        Concept concept = getIncomingNeighbour(Schema.EdgeLabel.HAS_ROLE);

        if(concept == null){
            return null;
        } else {
            return concept.asRelationType();
        }
    }

    /**
     *
     * @return A list of all the Concept Types which can play this role.
     */
    @Override
    public Collection<Type> playedByTypes() {
        Collection<Type> types = new HashSet<>();
        getSubHierarchySuperSet().forEach(r -> {
            r.getIncomingNeighbours(Schema.EdgeLabel.PLAYS_ROLE).forEach(c -> types.add(c.asType()));
        });
        return types;
    }

    /**
     *
     * @return All the instances of this type.
     */
    @Override
    public Collection<Instance> instances(){
        return Collections.emptyList();
    }

    /**
     *
     * @return The castings of this role
     */
    public Set<CastingImpl> castings(){
        Set<CastingImpl> castings = new HashSet<>();
        getIncomingNeighbours(Schema.EdgeLabel.ISA).forEach(concept -> ((CastingImpl) concept).getRelations().forEach(relation -> getMindmapsGraph().getConceptLog().putConcept(relation)));
        return castings;
    }
}
