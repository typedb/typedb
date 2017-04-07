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
import java.util.Set;
import java.util.stream.Collectors;

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
    private ComponentCache<Set<Type>> cachedDirectPlayedByTypes = new ComponentCache<>(() -> this.<Type>getIncomingNeighbours(Schema.EdgeLabel.PLAYS).collect(Collectors.toSet()));
    private ComponentCache<Set<RelationType>> cachedRelationTypes = new ComponentCache<>(() -> this.<RelationType>getIncomingNeighbours(Schema.EdgeLabel.RELATES).collect(Collectors.toSet()));

    RoleTypeImpl(AbstractGraknGraph graknGraph, Vertex v) {
        super(graknGraph, v);
    }

    RoleTypeImpl(AbstractGraknGraph graknGraph, Vertex v, RoleType type) {
        super(graknGraph, v, type);
    }

    RoleTypeImpl(AbstractGraknGraph graknGraph, Vertex v, RoleType type, Boolean isImplicit) {
        super(graknGraph, v, type, isImplicit);
    }

    private RoleTypeImpl(RoleTypeImpl role){
        super(role);
    }

    @Override
    public RoleType copy(){
        return new RoleTypeImpl(this);
    }

    @Override
    void copyCachedConcepts(RoleType type){
        super.copyCachedConcepts(type);
        ((RoleTypeImpl) type).cachedDirectPlayedByTypes.ifPresent(value -> this.cachedDirectPlayedByTypes.set(getGraknGraph().clone(value)));
        ((RoleTypeImpl) type).cachedRelationTypes.ifPresent(value -> this.cachedRelationTypes.set(getGraknGraph().clone(value)));
    }

    /**
     *
     * @return The Relation Type which this role takes part in.
     */
    @Override
    public Collection<RelationType> relationTypes() {
        return Collections.unmodifiableCollection(cachedRelationTypes.get());
    }

    /**
     * Caches a new relation type which this role will be part of. This may result in a DB hit if the cache has not been
     * initialised.
     *
     * @param newRelationType The new relation type to cache in the role.
     */
    void addCachedRelationType(RelationType newRelationType){
        cachedRelationTypes.ifPresent(set -> set.add(newRelationType));
    }

    /**
     * Removes an old relation type which this role is no longer part of. This may result in a DB hit if the cache has
     * not been initialised.
     *
     * @param oldRelationType The new relation type to cache in the role.
     */
    void deleteCachedRelationType(RelationType oldRelationType){
        cachedRelationTypes.ifPresent(set -> set.remove(oldRelationType));
    }

    /**
     *
     * @return A list of all the Concept Types which can play this role.
     */
    @Override
    public Collection<Type> playedByTypes() {
        Set<Type> playedByTypes = new HashSet<>();
        cachedDirectPlayedByTypes.get().forEach(type -> playedByTypes.addAll(type.subTypes()));
        return Collections.unmodifiableCollection(playedByTypes);
    }

    void addCachedDirectPlaysByType(Type newType){
        cachedDirectPlayedByTypes.ifPresent(set -> set.add(newType));
    }

    void deleteCachedDirectPlaysByType(Type oldType){
        cachedDirectPlayedByTypes.ifPresent(set -> set.remove(oldType));
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
        getIncomingNeighbours(Schema.EdgeLabel.ISA).forEach(concept -> castings.add((CastingImpl) concept));
        return castings;
    }

    /**
     *
     * @param roleType The Role Type which the instances of this Type are allowed to play.
     * @return The Type itself.
     */
    @Override
    public RoleType plays(RoleType roleType) {
        if(equals(roleType)){
            throw new ConceptException(ErrorMessage.ROLE_TYPE_ERROR.getMessage(roleType.getLabel()));
        }
        return super.plays(roleType, false);
    }

    @Override
    public void delete(){
        boolean hasRelates = getVertex().edges(Direction.IN, Schema.EdgeLabel.RELATES.getLabel()).hasNext();
        boolean hasPlays = getVertex().edges(Direction.IN, Schema.EdgeLabel.PLAYS.getLabel()).hasNext();

        if(hasRelates || hasPlays){
            throw new ConceptException(ErrorMessage.CANNOT_DELETE.getMessage(getLabel()));
        } else {
            super.delete();

            //Clear all internal caching
            cachedRelationTypes.clear();
            cachedDirectPlayedByTypes.clear();
        }
    }

}
