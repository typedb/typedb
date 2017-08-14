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

package ai.grakn.graql.internal.reasoner.utils.conversion;

import ai.grakn.concept.RelationType;
import ai.grakn.concept.Role;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * <p>
 * Implementation of {@link OntologyConceptConverter} allowing for conversion of role types to compatible types.
 * </p>
 *
 * @author Kasper Piskorski
 */
public class RoleTypeConverter implements OntologyConceptConverter<Role> {
    @Override
    public Multimap<RelationType, Role> toRelationMultimap(Role role) {
        Multimap<RelationType, Role> relationMap = HashMultimap.create();
        role.subs().forEach(roleType -> {
                    roleType.relationTypes()
                            .filter(rel -> !rel.isImplicit())
                            .forEach(rel -> relationMap.put(rel, roleType));
                });
        return relationMap;
    }
}
