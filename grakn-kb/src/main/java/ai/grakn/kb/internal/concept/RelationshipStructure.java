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

package ai.grakn.kb.internal.concept;

import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Thing;
import ai.grakn.kb.internal.cache.CacheOwner;
import ai.grakn.kb.internal.structure.EdgeElement;
import ai.grakn.kb.internal.structure.VertexElement;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * <p>
 *     Encapsulates The structure of a  {@link Relationship}.
 * </p>
 *
 * <p>
 *     This wraps up the structure of a {@link Relationship} as either a {@link RelationshipReified} or a
 *     {@link RelationshipEdge}.
 *     It contains methods which can be accessed regardless of the {@link Relationship} being a represented by a
 *     {@link VertexElement} or an {@link EdgeElement}
 * </p>
 *
 * @author fppt
 *
 */
interface RelationshipStructure extends CacheOwner{

    /**
     *
     * @return The {@link ConceptId} of the {@link Relationship}
     */
    ConceptId id();

    /**
     * Used for determining which {@link Keyspace} a {@link RelationshipStructure} was created in and is bound to.
     *
     * @return The {@link Keyspace} this {@link RelationshipStructure} belongs to.
     */
    Keyspace keyspace();

    /**
     *
     * @return The relation structure which has been reified
     */
    RelationshipReified reify();

    /**
     *
     * @return true if the {@link Relationship} has been reified meaning it can support n-ary relationships
     */
    boolean isReified();

    /**
     *
     * @return The {@link RelationshipType} of the {@link Relationship}
     */
    RelationshipType type();

    /**
     *
     * @return All the {@link Role}s and the {@link Thing}s which play them
     */
    Map<Role, Set<Thing>> allRolePlayers();

    /**
     *
     * @param roles The {@link Role}s which are played in this relation
     * @return The {@link Thing}s which play those {@link Role}s
     */
    Stream<Thing> rolePlayers(Role... roles);

    /**
     * Deletes the {@link VertexElement} or {@link EdgeElement} used to represent this {@link Relationship}
     */
    void delete();

    /**
     * Return whether the relationship has been deleted.
     */
    boolean isDeleted();

    /**
     * Used to indicate if this {@link Relationship} has been created as the result of a {@link Rule} inference.
     * @see Rule
     *
     * @return true if this {@link Relationship} exists due to a rule
     */
    boolean isInferred();

}
