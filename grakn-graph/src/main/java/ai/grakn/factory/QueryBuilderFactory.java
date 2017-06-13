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
package ai.grakn.factory;

import org.apache.tinkerpop.gremlin.structure.Graph;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.graql.QueryBuilder;

/**
 * <p>
 * A <code>QueryBuilderFactory</code> can produce a {@link ai.grakn.graql.QueryBuilder} 
 * from a graph
 * </p>
 * @author borislav
 * @param <T> The concrete type of the underlying graph implementation
 */
public interface QueryBuilderFactory<T extends Graph> {
    /**
     * Construct a new {@link ai.grakn.graql.QueryBuilder}
     * 
     * @param graph The graph which will be queried.
     * @return A query builder bound to the specified graph.
     */
    QueryBuilder getQueryBuilder(AbstractGraknGraph<T> graph);
}