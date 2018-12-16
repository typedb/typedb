/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.server.kb.concept;

import grakn.core.common.util.CommonUtil;
import grakn.core.graql.concept.RelationshipType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.concept.Type;
import grakn.core.graql.internal.Schema;
import grakn.core.server.kb.cache.Cache;
import grakn.core.server.kb.cache.Cacheable;
import grakn.core.server.kb.structure.Casting;
import grakn.core.server.kb.structure.VertexElement;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <p>
 *     An {@link SchemaConcept} which defines a {@link Role} which can be played in a {@link RelationshipType}.
 * </p>
 *
 * <p>
 *     This {@link SchemaConcept} defines the roles which make up a {@link RelationshipType}.
 *     It has some additional functionality:
 *     1. It cannot play a {@link Role} to itself.
 *     2. It is special in that it is unique to {@link RelationshipType}s.
 * </p>
 *
 *
 */
public class RoleImpl extends SchemaConceptImpl<Role> implements Role {
    private final Cache<Set<Type>> cachedDirectPlayedByTypes = Cache.createSessionCache(this, Cacheable.set(), () -> this.<Type>neighbours(Direction.IN, Schema.EdgeLabel.PLAYS).collect(Collectors.toSet()));
    private final Cache<Set<RelationshipType>> cachedRelationTypes = Cache.createSessionCache(this, Cacheable.set(), () -> this.<RelationshipType>neighbours(Direction.IN, Schema.EdgeLabel.RELATES).collect(Collectors.toSet()));

    private RoleImpl(VertexElement vertexElement) {
        super(vertexElement);
    }

    private RoleImpl(VertexElement vertexElement, Role type) {
        super(vertexElement, type);
    }

    public static RoleImpl get(VertexElement vertexElement){
        return new RoleImpl(vertexElement);
    }

    public static RoleImpl create(VertexElement vertexElement, Role type) {
        RoleImpl role = new RoleImpl(vertexElement, type);
        vertexElement.tx().cache().trackForValidation(role);
        return role;
    }

    @Override
    public Stream<RelationshipType> relationships() {
        return cachedRelationTypes.get().stream();
    }

    /**
     * Caches a new relation type which this role will be part of. This may result in a DB hit if the cache has not been
     * initialised.
     *
     * @param newRelationshipType The new relation type to cache in the role.
     */
    void addCachedRelationType(RelationshipType newRelationshipType){
        cachedRelationTypes.ifPresent(set -> set.add(newRelationshipType));
    }

    /**
     * Removes an old relation type which this role is no longer part of. This may result in a DB hit if the cache has
     * not been initialised.
     *
     * @param oldRelationshipType The new relation type to cache in the role.
     */
    void deleteCachedRelationType(RelationshipType oldRelationshipType){
        cachedRelationTypes.ifPresent(set -> set.remove(oldRelationshipType));
    }

    /**
     *
     * @return A list of all the Concept Types which can play this role.
     */
    @Override
    public Stream<Type> players() {
        return cachedDirectPlayedByTypes.get().stream().flatMap(Type::subs);
    }

    void addCachedDirectPlaysByType(Type newType){
        cachedDirectPlayedByTypes.ifPresent(set -> set.add(newType));
    }

    void deleteCachedDirectPlaysByType(Type oldType){
        cachedDirectPlayedByTypes.ifPresent(set -> set.remove(oldType));
    }

    /**
     *
     * @return Get all the roleplayers of this role type
     */
    public Stream<Casting> rolePlayers(){
        return relationships().
                flatMap(RelationshipType::instances).
                map(relation -> RelationshipImpl.from(relation).reified()).
                flatMap(CommonUtil::optionalToStream).
                flatMap(relation -> relation.castingsRelation(this));
    }

    @Override
    boolean deletionAllowed(){
        return super.deletionAllowed() &&
                !neighbours(Direction.IN, Schema.EdgeLabel.RELATES).findAny().isPresent() && // This role is not linked t any relation type
                !neighbours(Direction.IN, Schema.EdgeLabel.PLAYS).findAny().isPresent() && // Nothing can play this role
                !rolePlayers().findAny().isPresent(); // This role has no role players
    }

    @Override
    void trackRolePlayers() {
        //TODO: track the super change when the role super changes
    }

}
