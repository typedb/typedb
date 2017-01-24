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

import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.exception.ConceptException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * <p>
 *     An ontological element which defines a role which can be played in a relation type.
 * </p>
 *
 * <p>
 *     This ontological element defines the roles which make up a {@link RelationType}.
 *     It behaves similarly to {@link Type} when relating to other types.
 *     It has some additional functionality:
 *     1. It cannot play a role to itself.
 *     2. It is special in that it is unique to relation types.
 * </p>
 *
 * @author fppt
 *
 */
class RoleTypeImpl extends TypeImpl<RoleType, Instance> implements RoleType{
    RoleTypeImpl(AbstractGraknGraph graknGraph, Vertex v, Optional<RoleType> type, Optional<Boolean> isImplicit) {
        super(graknGraph, v, type, isImplicit);
    }

    /**
     *
     * @return The Relation Type which this role takes part in.
     */
    @Override
    public Collection<RelationType> relationTypes() {
        return getIncomingNeighbours(Schema.EdgeLabel.HAS_ROLE);
    }

    /**
     *
     * @return A list of all the Concept Types which can play this role.
     */
    @Override
    public Collection<Type> playedByTypes() {
        Set<Type> playedBy = new HashSet<>();
        getIncomingNeighbours(Schema.EdgeLabel.PLAYS_ROLE).
                forEach(concept -> playedBy.addAll(concept.asType().subTypes()));
        return playedBy;
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
        getIncomingNeighbours(Schema.EdgeLabel.ISA).forEach(concept -> ((CastingImpl) concept).getRelations().forEach(relation -> getGraknGraph().getConceptLog().putConcept(relation)));
        return castings;
    }

    /**
     *
     * @param roleType The Role Type which the instances of this Type are allowed to play.
     * @return The Type itself.
     */
    @Override
    public RoleType playsRole(RoleType roleType) {
        if(equals(roleType)){
            throw new ConceptException(ErrorMessage.ROLE_TYPE_ERROR.getMessage(roleType.getName()));
        }
        return super.playsRole(roleType, false);
    }

    @Override
    public void innerDelete(){
        boolean hasHasRoles = getVertex().edges(Direction.IN, Schema.EdgeLabel.HAS_ROLE.getLabel()).hasNext();
        boolean hasPlaysRoles = getVertex().edges(Direction.IN, Schema.EdgeLabel.PLAYS_ROLE.getLabel()).hasNext();

        if(hasHasRoles || hasPlaysRoles){
            throw new ConceptException(ErrorMessage.CANNOT_DELETE.getMessage(getName()));
        } else {
            super.innerDelete();
        }
    }

}
