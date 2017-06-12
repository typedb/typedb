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

import ai.grakn.concept.Concept;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

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
    private Cache<Set<RoleType>> cachedRelates = new Cache<>(() -> this.<RoleType>neighbours(Direction.OUT, Schema.EdgeLabel.RELATES).collect(Collectors.toSet()));

    RelationTypeImpl(VertexElement vertexElement) {
        super(vertexElement);
    }

    RelationTypeImpl(VertexElement vertexElement, RelationType type, Boolean isImplicit) {
        super(vertexElement, type, isImplicit);
    }

    @Override
    public Relation addRelation() {
        return addInstance(Schema.BaseType.RELATION,
                (vertex, type) -> vertex().graph().factory().buildRelation(vertex, type));
    }

    @Override
    public void flushTxCache(){
        super.flushTxCache();
        cachedRelates.flush();
    }

    /**
     *
     * @return A list of the Role Types which make up this Relation Type.
     */
    @Override
    public Collection<RoleType> relates() {
        return Collections.unmodifiableCollection(cachedRelates.get());
    }

    /**
     *
     * @param roleType A new role which is part of this relationship.
     * @return The Relation Type itself.
     */
    @Override
    public RelationType relates(RoleType roleType) {
        checkTypeMutationAllowed();
        putEdge(roleType, Schema.EdgeLabel.RELATES);

        //Cache the Role internally
        cachedRelates.ifPresent(set -> set.add(roleType));

        //Cache the relation type in the role
        ((RoleTypeImpl) roleType).addCachedRelationType(this);

        //Put all the instance back in for tracking because their unique hashes need to be regenerated
        instances().forEach(instance -> vertex().graph().txCache().trackForValidation((ConceptImpl) instance));

        return this;
    }

    /**
     *
     * @param roleType The role type to delete from this relationship.
     * @return The Relation Type itself.
     */
    @Override
    public RelationType deleteRelates(RoleType roleType) {
        checkTypeMutationAllowed();
        deleteEdge(Direction.OUT, Schema.EdgeLabel.RELATES, (Concept) roleType);

        RoleTypeImpl roleTypeImpl = (RoleTypeImpl) roleType;
        //Add roleplayers of roleType to make sure relations are still valid
        roleTypeImpl.rolePlayers().forEach(rolePlayer -> vertex().graph().txCache().trackForValidation(rolePlayer));


        //Add the Role Type itself
        vertex().graph().txCache().trackForValidation(roleTypeImpl);

        //Add the Relation Type
        vertex().graph().txCache().trackForValidation(roleTypeImpl);

        //Remove from internal cache
        cachedRelates.ifPresent(set -> set.remove(roleType));

        //Remove from roleTypeCache
        ((RoleTypeImpl) roleType).deleteCachedRelationType(this);

        //Put all the instance back in for tracking because their unique hashes need to be regenerated
        instances().forEach(instance -> vertex().graph().txCache().trackForValidation((ConceptImpl) instance));

        return this;
    }

    @Override
    public void delete(){
        //Force load the cache
        cachedRelates.get();

        super.delete();

        //Update the cache of the connected role types
        cachedRelates.get().forEach(roleType -> ((RoleTypeImpl) roleType).deleteCachedRelationType(this));

        //Clear internal Cache
        cachedRelates.clear();
    }
}
