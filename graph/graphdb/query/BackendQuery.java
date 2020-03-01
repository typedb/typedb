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
 */

package grakn.core.graph.graphdb.query;

/**
 * A BackendQuery is a query that can be updated to a new limit.
 * <p>
 * This is useful in query execution where the query limit is successively relaxed to find all the needed elements
 * of the result set.
 */
public interface BackendQuery<Q extends BackendQuery> extends Query {

    /**
     * Creates a new query identical to the current one but with the specified limit.
     */
    Q updateLimit(int newLimit);

}
