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

import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Relation;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Rule;
import grakn.core.graql.concept.Thing;
import grakn.core.server.kb.cache.CacheOwner;
import grakn.core.server.kb.structure.EdgeElement;
import grakn.core.server.kb.structure.VertexElement;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * <p>
 *     Encapsulates The structure of a  {@link Relation}.
 * </p>
 *
 * <p>
 *     This wraps up the structure of a {@link Relation} as either a {@link RelationshipReified} or a
 *     {@link RelationshipEdge}.
 *     It contains methods which can be accessed regardless of the {@link Relation} being a represented by a
 *     {@link VertexElement} or an {@link EdgeElement}
 * </p>
 *
 *
 */
interface RelationshipStructure extends CacheOwner {

    /**
     *
     * @return The {@link ConceptId} of the {@link Relation}
     */
    ConceptId id();

    /**
     *
     * @return The relation structure which has been reified
     */
    RelationshipReified reify();

    /**
     *
     * @return true if the {@link Relation} has been reified meaning it can support n-ary relationships
     */
    boolean isReified();

    /**
     *
     * @return The {@link RelationType} of the {@link Relation}
     */
    RelationType type();

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
     * Deletes the {@link VertexElement} or {@link EdgeElement} used to represent this {@link Relation}
     */
    void delete();

    /**
     * Return whether the relationship has been deleted.
     */
    boolean isDeleted();

    /**
     * Used to indicate if this {@link Relation} has been created as the result of a {@link Rule} inference.
     * @see Rule
     *
     * @return true if this {@link Relation} exists due to a rule
     */
    boolean isInferred();

}
