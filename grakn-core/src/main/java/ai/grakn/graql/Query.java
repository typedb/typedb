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

package ai.grakn.graql;

import ai.grakn.GraknTx;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;

/**
 * A Graql query of any kind. May read and write to the graph.
 *
 * @param <T> The result type after executing the query
 *
 * @author Grakn Warriors
 */
public interface Query<T> {

    /**
     * @param tx the graph to execute the query on
     * @return a new query with the graph set
     */
    @CheckReturnValue
    Query<T> withTx(GraknTx tx);

    /**
     * Execute the query against the graph (potentially writing to the graph) and return a result
     * @return the result of the query
     */
    T execute();

    /**
     * Whether this query will modify the graph
     */
    @CheckReturnValue
    boolean isReadOnly();

    /**
     * Get the transaction associated with this query
     */
    @Nullable
    GraknTx tx();

    Boolean inferring();
}
