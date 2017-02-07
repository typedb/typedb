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
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * <p>
 *     An ontological element which categorises how instances may relate to each other.
 * </p>
 *
 * <p>
 *     A relation type defines how {@link ai.grakn.concept.Type} may relate to one another.
 *     They are used to model and categorise n-ary relationships.
 * </p>
 *
 * @author fppt
 *
 */
class RelationTypeImpl extends TypeImpl<RelationType, Relation> implements RelationType {
    RelationTypeImpl(AbstractGraknGraph graknGraph, Vertex v) {
        super(graknGraph, v);
    }

    RelationTypeImpl(AbstractGraknGraph graknGraph, Vertex v, RelationType type) {
        super(graknGraph, v, type);
    }

    RelationTypeImpl(AbstractGraknGraph graknGraph, Vertex v, RelationType type, Boolean isImplicit) {
        super(graknGraph, v, type, isImplicit);
    }

    @Override
    public Relation addRelation() {
        return addInstance(Schema.BaseType.RELATION,
                (vertex, type) -> getGraknGraph().getElementFactory().buildRelation(vertex, type));
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

        RoleTypeImpl roleTypeImpl = (RoleTypeImpl) roleType;
        //Add castings of roleType to make sure relations are still valid
        roleTypeImpl.castings().forEach(casting -> getGraknGraph().getConceptLog().trackConceptForValidation(casting));

        //Add the Role Type itself
        getGraknGraph().getConceptLog().trackConceptForValidation(roleTypeImpl);

        //Add the Relation Type
        getGraknGraph().getConceptLog().trackConceptForValidation(roleTypeImpl);

        return this;
    }
}
