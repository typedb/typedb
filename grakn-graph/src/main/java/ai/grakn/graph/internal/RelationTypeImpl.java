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
import java.util.Collections;
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
    private Cache<Set<RoleType>> cachedHasRoles = new Cache<>(() -> getOutgoingNeighbours(Schema.EdgeLabel.HAS_ROLE));

    RelationTypeImpl(AbstractGraknGraph graknGraph, Vertex v) {
        super(graknGraph, v);
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
        return Collections.unmodifiableCollection(cachedHasRoles.get());
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

        //Cache the Role internally
        cachedHasRoles.ifPresent(set -> set.add(roleType));

        //Cache the relation type in the role
        ((RoleTypeImpl) roleType).addCachedRelationType(this);

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

        //Remove from internal cache
        cachedHasRoles.ifPresent(set -> set.remove(roleType));

        //Remove from roleTypeCache
        ((RoleTypeImpl) roleType).deleteCachedRelationType(this);

        return this;
    }

    @Override
    public void delete(){
        //Force load the cache
        cachedHasRoles.get();

        super.delete();

        //Update the cache of the connected role types
        cachedHasRoles.get().forEach(roleType -> ((RoleTypeImpl) roleType).deleteCachedRelationType(this));

        //Clear internal Cache
        cachedHasRoles.clear();
    }
}
