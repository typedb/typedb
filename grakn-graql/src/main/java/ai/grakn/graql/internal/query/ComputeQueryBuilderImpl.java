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

package ai.grakn.graql.internal.query;

import ai.grakn.GraknGraph;
import ai.grakn.graql.ComputeQueryBuilder;
import ai.grakn.graql.analytics.*;
import ai.grakn.graql.internal.query.analytics.*;

import java.util.Optional;

public class ComputeQueryBuilderImpl implements ComputeQueryBuilder {

    private Optional<GraknGraph> graph;

    ComputeQueryBuilderImpl(Optional<GraknGraph> graph) {
        this.graph = graph;
    }

    @Override
    public ComputeQueryBuilder withGraph(GraknGraph graph) {
        this.graph = Optional.of(graph);
        return this;
    }

    @Override
    public CountQuery count() {
        return new CountQueryImpl(graph);
    }

    @Override
    public MinQuery min() {
        return new MinQueryImpl(graph);
    }

    @Override
    public MaxQuery max() {
        return new MaxQueryImpl(graph);
    }

    @Override
    public SumQuery sum() {
        return new SumQueryImpl(graph);
    }

    @Override
    public MeanQuery mean() {
        return new MeanQueryImpl(graph);
    }

    @Override
    public StdQuery std() {
        return new StdQueryImpl(graph);
    }

    @Override
    public MedianQuery median() {
        return new MedianQueryImpl(graph);
    }

    @Override
    public PathQuery path() {
        return new PathQueryImpl(graph);
    }
}
