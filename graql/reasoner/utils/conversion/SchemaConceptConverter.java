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
package grakn.core.graql.reasoner.utils.conversion;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.SchemaConcept;

import java.util.stream.Stream;

/**
 * <p>
 * Interface for conversion between compatible SchemaConcepts.
 * NB: assumes MATCH semantics - all types and their subs are considered.
 * </p>
 * @param <T> SchemaConcept type to convert from
 *
 */
public interface SchemaConceptConverter<T extends SchemaConcept>{

    /**
     * convert a given type to a map of relation types in which it can play roles
     * and the corresponding role types including entity type hierarchy
     * @param entryConcept to be converted
     * @return map of relation types in which it can play roles and the corresponding role types
     */
    default Multimap<RelationType, Role> toRelationMultimap(T entryConcept){
        Multimap<RelationType, Role> relationMap = HashMultimap.create();

        toCompatibleRoles(entryConcept)
                .forEach(role -> role.relations()
                        .forEach(rel -> relationMap.put(rel, role)));
        return relationMap;
    }

    /**
     * @param entryConcept to be converted
     * @return Roles that are compatible with this SchemaConcept
     */
    Stream<Role> toCompatibleRoles(T entryConcept);
}

