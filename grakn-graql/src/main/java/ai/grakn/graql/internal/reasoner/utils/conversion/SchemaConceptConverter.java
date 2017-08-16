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

import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Role;
import com.google.common.collect.Multimap;

/**
 * <p>
 *      {@link SchemaConceptConverter} interface for conversion between compatible {@link SchemaConcept}.
 * </p>
 * @param <T> type to convert from
 *
 * @author Kasper Piskorski
 */
public interface SchemaConceptConverter<T extends SchemaConcept>{
    /**
     * convert a given type to a map of relation types in which it can play roles
     * and the corresponding role types including entity type hierarchy
     * @param schemaConcept to be converted
     * @return map of relation types in which it can play roles and the corresponding role types
     */
    Multimap<RelationshipType, Role> toRelationshipMultimap(T schemaConcept);
}

