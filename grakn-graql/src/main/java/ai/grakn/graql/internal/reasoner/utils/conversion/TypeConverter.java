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

import ai.grakn.concept.Concept;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Type;
import ai.grakn.graql.internal.reasoner.utils.ReasonerUtils;
import ai.grakn.util.Schema;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import java.util.Set;

/**
 * <p>
 * Basic {@link SchemaConceptConverter} implementation for conversion between compatible types.
 * </p>
 *
 * @author Kasper Piskorski
 */
public class TypeConverter implements SchemaConceptConverter<Type> {

    @Override
    public Multimap<RelationshipType, Role> toRelationshipMultimap(Type entryType) {
        Multimap<RelationshipType, Role> relationMap = HashMultimap.create();

        Set<Type> typeHierarchy = Sets.newHashSet(entryType);
        if (Schema.MetaSchema.isMetaLabel(entryType.getLabel())) {
            entryType.subs().forEach(typeHierarchy::add);
        } else {
            ReasonerUtils.supers(entryType).stream().map(Concept::asType).forEach(typeHierarchy::add);
        }

        typeHierarchy.stream()
                .flatMap(Type::plays)
                .forEach(roleType -> {
                    roleType.relationshipTypes()
                            .filter(rel -> !rel.isImplicit())
                            .forEach(rel -> relationMap.put(rel, roleType));
                });
        return relationMap;
    }
}
