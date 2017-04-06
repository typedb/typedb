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
    private ComponentCache<Set<RoleType>> cachedRelates = new ComponentCache<>(() -> this.<RoleType>getOutgoingNeighbours(Schema.EdgeLabel.RELATES).collect(Collectors.toSet()));

    RelationTypeImpl(AbstractGraknGraph graknGraph, Vertex v) {
        super(graknGraph, v);
    }

    RelationTypeImpl(AbstractGraknGraph graknGraph, Vertex v, RelationType type, Boolean isImplicit) {
        super(graknGraph, v, type, isImplicit);
    }

    private RelationTypeImpl(RelationTypeImpl relationType){
        super(relationType);
    }

    @Override
    public RelationType copy(){
        return new RelationTypeImpl(this);
    }

    @Override
    void copyCachedConcepts(RelationType type){
        super.copyCachedConcepts(type);
        ((RelationTypeImpl) type).cachedRelates.ifPresent(value -> this.cachedRelates.set(getGraknGraph().clone(value)));
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
        checkTypeMutation();
        putEdge(roleType, Schema.EdgeLabel.RELATES);

        //ComponentCache the Role internally
        cachedRelates.ifPresent(set -> set.add(roleType));

        //ComponentCache the relation type in the role
        ((RoleTypeImpl) roleType).addCachedRelationType(this);

        //Put all the instance back in for tracking because their unique hashes need to be regenerated
        instances().forEach(instance -> getGraknGraph().getConceptLog().trackConceptForValidation((ConceptImpl) instance));

        return this;
    }

    /**
     *
     * @param roleType The role type to delete from this relationship.
     * @return The Relation Type itself.
     */
    @Override
    public RelationType deleteRelates(RoleType roleType) {
        checkTypeMutation();
        deleteEdgeTo(Schema.EdgeLabel.RELATES, roleType);

        RoleTypeImpl roleTypeImpl = (RoleTypeImpl) roleType;
        //Add castings of roleType to make sure relations are still valid
        roleTypeImpl.castings().forEach(casting -> getGraknGraph().getConceptLog().trackConceptForValidation(casting));

        //Add the Role Type itself
        getGraknGraph().getConceptLog().trackConceptForValidation(roleTypeImpl);

        //Add the Relation Type
        getGraknGraph().getConceptLog().trackConceptForValidation(roleTypeImpl);

        //Remove from internal cache
        cachedRelates.ifPresent(set -> set.remove(roleType));

        //Remove from roleTypeCache
        ((RoleTypeImpl) roleType).deleteCachedRelationType(this);

        //Put all the instance back in for tracking because their unique hashes need to be regenerated
        instances().forEach(instance -> getGraknGraph().getConceptLog().trackConceptForValidation((ConceptImpl) instance));

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
