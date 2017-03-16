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
import ai.grakn.concept.Relation;
import ai.grakn.concept.RoleType;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * <p>
 *     Internal class representing links between instances and roles.
 * </p>
 *
 * <p>
 *     An internal concept used to represent the link between an {@link Instance} and it's {@link RoleType}.
 *     For example Pacino as an actor would be represented by a single casting regardless of the number of movies he acts in.
 * </p>
 *
 * @author fppt
 */
class CastingImpl extends InstanceImpl<CastingImpl, RoleType> {

    CastingImpl(AbstractGraknGraph graknGraph, Vertex v) {
        super(graknGraph, v);
    }

    CastingImpl(AbstractGraknGraph graknGraph, Vertex v, RoleType type) {
        super(graknGraph, v, type);
    }

    /**
     *
     * @return The {@link RoleType} this casting is linked with
     */
    public RoleType getRole() {
       return type();
    }

    /**
     *
     * @return The {@link Instance} which is the roleplayer in this casting
     */
    Instance getRolePlayer() {
        return this.<Instance>getOutgoingNeighbours(Schema.EdgeLabel.ROLE_PLAYER).findFirst().orElse(null);
    }

    /**
     * Sets thew internal index of this casting toi allow for faster lookup.
     * @param role The {@link RoleType} this casting is linked with
     * @param rolePlayer The {@link Instance} which is the roleplayer in this casting
     * @return The casting itself.
     */
    public CastingImpl setHash(RoleTypeImpl role, InstanceImpl rolePlayer){
        String hash;
        if(getGraknGraph().isBatchLoadingEnabled()) {
            hash = "CastingBaseId_" + this.getId().getValue() + UUID.randomUUID().toString();
        } else {
            hash = generateNewHash(role, rolePlayer);
        }
        setUniqueProperty(Schema.ConceptProperty.INDEX, hash);
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
    public Set<Relation> getRelations() {
        return this.<Relation>getIncomingNeighbours(Schema.EdgeLabel.CASTING).collect(Collectors.toSet());
    }
}
