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

package ai.grakn.graql;

import ai.grakn.GraknGraph;
import ai.grakn.graql.analytics.ClusterQuery;
import ai.grakn.graql.analytics.CountQuery;
import ai.grakn.graql.analytics.DegreeQuery;
import ai.grakn.graql.analytics.MaxQuery;
import ai.grakn.graql.analytics.MeanQuery;
import ai.grakn.graql.analytics.MedianQuery;
import ai.grakn.graql.analytics.MinQuery;
import ai.grakn.graql.analytics.PathQuery;
import ai.grakn.graql.analytics.StdQuery;
import ai.grakn.graql.analytics.SumQuery;

import java.util.Map;

/**
 * Starting point for creating compute queries.
 *
 * @author Jason Liu
 */
public interface ComputeQueryBuilder {

    /**
     * @param graph the graph to execute the compute query on
     * @return a compute query builder with the graph set
     */
    ComputeQueryBuilder withGraph(GraknGraph graph);

    /**
     * @return a count query that will count the number of instances
     */
    CountQuery count();

    /**
     * @return a min query that will find the min value of the given resource types
     */
    MinQuery min();

    /**
     * @return a max query that will find the max value of the given resource types
     */
    MaxQuery max();

    /**
     * @return a sum query that will compute the sum of values of the given resource types
     */
    SumQuery sum();

    /**
     * @return a mean query that will compute the mean of values of the given resource types
     */
    MeanQuery mean();

    /**
     * @return a std query that will compute the standard deviation of values of the given resource types
     */
    StdQuery std();

    /**
     * @return a median query that will compute the median of values of the given resource types
     */
    MedianQuery median();

    /**
     * @return a path query that will find the shortest path between two instances
     */
    PathQuery path();

    /**
     * @return a cluster query that will find the clusters in the graph
     */
    ClusterQuery<Map<String, Long>> cluster();

    /**
     * @return a degree query that will compute the degree of instances
     */
    DegreeQuery degree();
}
