/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.concept.impl;

import grakn.core.concept.cache.ConceptCache;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.concept.manager.ConceptNotificationChannel;
import grakn.core.kb.concept.structure.Casting;
import grakn.core.kb.concept.structure.VertexElement;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An SchemaConcept which defines a Role which can be played in a RelationType.
 * This SchemaConcept defines the roles which make up a RelationType.
 * It has some additional functionality:
 * 1. It cannot play a Role to itself.
 * 2. It is special in that it is unique to RelationTypes.
 */
public class RoleImpl extends SchemaConceptImpl<Role> implements Role {
    private final ConceptCache<Set<Type>> cachedDirectPlayedByTypes = new ConceptCache<>(() -> this.<Type>neighbours(Direction.IN, Schema.EdgeLabel.PLAYS).collect(Collectors.toSet()));
    private final ConceptCache<Set<RelationType>> cachedRelationTypes = new ConceptCache<>(() -> this.<RelationType>neighbours(Direction.IN, Schema.EdgeLabel.RELATES).collect(Collectors.toSet()));

    public RoleImpl(VertexElement vertexElement, ConceptManager conceptManager, ConceptNotificationChannel conceptNotificationChannel) {
        super(vertexElement, conceptManager, conceptNotificationChannel);
    }

    @Override
    public Stream<RelationType> relations() {
        return cachedRelationTypes.get().stream();
    }

    /**
     * Caches a new relation type which this role will be part of. This may result in a DB hit if the cache has not been
     * initialised.
     *
     * @param newRelationType The new relation type to cache in the role.
     */
    void addCachedRelationType(RelationType newRelationType) {
        cachedRelationTypes.ifCached(set -> set.add(newRelationType));
    }

    /**
     * Removes an old relation type which this role is no longer part of. This may result in a DB hit if the cache has
     * not been initialised.
     *
     * @param oldRelationType The new relation type to cache in the role.
     */
    void deleteCachedRelationType(RelationType oldRelationType) {
        cachedRelationTypes.ifCached(set -> set.remove(oldRelationType));
    }

    /**
     * @return A list of all the Concept Types which can play this role.
     */
    @Override
    public Stream<Type> players() {
        return cachedDirectPlayedByTypes.get().stream().flatMap(Type::subs);
    }

    void addCachedDirectPlaysByType(Type newType) {
        cachedDirectPlayedByTypes.ifCached(set -> set.add(newType));
    }

    void deleteCachedDirectPlaysByType(Type oldType) {
        cachedDirectPlayedByTypes.ifCached(set -> set.remove(oldType));
    }

    /**
     * @return Get all the roleplayers of this role type
     */
    public Stream<Casting> rolePlayers() {
        return relations()
                .flatMap(RelationType::instances)
                .map(relation -> RelationImpl.from(relation).reified())
                .filter(Objects::nonNull)
                .flatMap(relation -> relation.castingsRelation(this));
    }

    @Override
    boolean deletionAllowed() {
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
