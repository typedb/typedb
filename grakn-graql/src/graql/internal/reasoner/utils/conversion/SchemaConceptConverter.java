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
package grakn.core.graql.internal.reasoner.utils.conversion;

import grakn.core.concept.RelationshipType;
import grakn.core.concept.SchemaConcept;
import grakn.core.concept.Role;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.stream.Stream;

/**
 * <p>
 * Interface for conversion between compatible {@link SchemaConcept}s.
 * NB: assumes MATCH semantics - all types and their subs are considered.
 * </p>
 * @param <T> {@link SchemaConcept} type to convert from
 *
 * @author Kasper Piskorski
 */
public interface SchemaConceptConverter<T extends SchemaConcept>{

    /**
     * convert a given type to a map of relation types in which it can play roles
     * and the corresponding role types including entity type hierarchy
     * @param entryConcept to be converted
     * @return map of relation types in which it can play roles and the corresponding role types
     */
    default Multimap<RelationshipType, Role> toRelationshipMultimap(T entryConcept){
        Multimap<RelationshipType, Role> relationMap = HashMultimap.create();

        toCompatibleRoles(entryConcept)
                .forEach(role -> role.relationships()
                        .forEach(rel -> relationMap.put(rel, role)));
        return relationMap;
    }

    /**
     * @param entryConcept to be converted
     * @return {@link Role}s that are compatible with this {@link SchemaConcept}
     */
    Stream<Role> toCompatibleRoles(T entryConcept);
}

