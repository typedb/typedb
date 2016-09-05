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

import io.mindmaps.constants.DataType;
import io.mindmaps.core.implementation.exception.NoEdgeException;
import io.mindmaps.core.model.*;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * An internal concept used to represent the link between a roleplayer and it's role.
 * For example Pacino as an actor would be represented by a single casting regardless of the number of movies he acts in.
 */
class CastingImpl extends ConceptImpl {

    CastingImpl(Vertex v, MindmapsTransactionImpl mindmapsGraph) {
        super(v, mindmapsGraph);
    }

    /**
     *
     * @return The {@link RoleType} this casting is linked with
     */
    public RoleTypeImpl getRole() {
        Concept concept = getParentIsa();
        if(concept != null)
            return getMindmapsTransaction().getElementFactory().buildRoleType(concept);
        else
            throw new NoEdgeException(toString(), DataType.BaseType.ROLE_TYPE.name());
    }

    /**
     *
     * @return The {@link Instance} which is the roleplayer in this casting
     */
    public InstanceImpl getRolePlayer() {
        Concept concept = getOutgoingNeighbour(DataType.EdgeLabel.ROLE_PLAYER);
        if(concept != null)
            return getMindmapsTransaction().getElementFactory().buildSpecificInstance(concept);
        else
            return null;
    }

    /**
     * Sets thew internal index of this casting toi allow for faster lookup.
     * @param role The {@link RoleType} this casting is linked with
     * @param rolePlayer The {@link Instance} which is the roleplayer in this casting
     * @return The casting itself.
     */
    public CastingImpl setHash(RoleTypeImpl role, InstanceImpl rolePlayer){
        String hash;
        if(getMindmapsTransaction().isBatchLoadingEnabled())
            hash = "CastingBaseId_" + this.getBaseIdentifier() + UUID.randomUUID().toString();
        else
            hash = generateNewHash(role, rolePlayer);
        setUniqueProperty(DataType.ConceptPropertyUnique.INDEX, hash);
        return this;
    }

    /**
     *
     * @param role The {@link RoleType} this casting is linked with
     * @param rolePlayer The {@link Instance} which is the roleplayer in this casting
     * @return A unique hash for the casting.
     */
    public static String generateNewHash(RoleTypeImpl role, InstanceImpl rolePlayer){
        return "Casting-Role-" + role.getId() + "-RolePlayer-" + rolePlayer.getId();
    }

    /**
     *
     * @return All the {@link Relation} this casting is linked with.
     */
    public Set<RelationImpl> getRelations() {
        ConceptImpl<?, ?> thisRef = this;
        Set<RelationImpl> relations = new HashSet<>();
        Set<ConceptImpl> concepts = thisRef.getIncomingNeighbours(DataType.EdgeLabel.CASTING);

        if(concepts.size() > 0){
            relations.addAll(concepts.stream().map(getMindmapsTransaction().getElementFactory()::buildRelation).collect(Collectors.toList()));
        }

        return relations;
    }
}
