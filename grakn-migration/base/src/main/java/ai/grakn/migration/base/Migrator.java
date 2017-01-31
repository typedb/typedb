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

package ai.grakn.migration.base;

import ai.grakn.graql.InsertQuery;

import java.util.stream.Stream;

/**
 * <p>
 *     The base Migrator interface
 * </p>
 *
 * <p>
 *     Provides common methods for migrating data from source into insert queries.
 * </p>
 *
 * @author alexandraorth
 */
public interface Migrator extends AutoCloseable {

    /**
     * Migrate all the data in the given file based on the given template.
     */
    Stream<InsertQuery> migrate();
}
