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

package ai.grakn.graql.admin;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Type;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

/**
 * Admin class for inspecting and manipulating an InsertQuery
 */
public interface InsertQueryAdmin extends InsertQuery {
    /**
     * @return the match query that this insert query is using, if it was provided one
     */
    Optional<? extends MatchQuery> getMatchQuery();

    /**
     * @return all concept types referred to explicitly in the query
     */
    Set<Type> getTypes();

    /**
     * @return the variables to insert in the insert query
     */
    Collection<VarAdmin> getVars();

    /**
     * @return the graph set on this query, if it was provided one
     */
    Optional<GraknGraph> getGraph();
}
