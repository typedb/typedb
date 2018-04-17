/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.utils.conversion;

import ai.grakn.concept.Role;
import ai.grakn.concept.Type;
import java.util.stream.Stream;

/**
 * <p>
 * Basic {@link SchemaConceptConverter} implementation for conversion between compatible types.
 * </p>
 *
 * @author Kasper Piskorski
 */
public class TypeConverter implements SchemaConceptConverter<Type> {

    @Override
    public Stream<Role> toCompatibleRoles(Type entryType) {
        return entryType.subs().flatMap(Type::plays);
    }
}
