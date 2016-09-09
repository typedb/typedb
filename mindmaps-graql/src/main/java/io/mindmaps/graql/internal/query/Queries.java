/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.query;

import com.google.common.collect.ImmutableCollection;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.graql.Aggregate;
import io.mindmaps.graql.AggregateQuery;
import io.mindmaps.graql.ComputeQuery;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.admin.*;
import io.mindmaps.graql.internal.query.match.MatchQueryBase;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Internal query factory
 */
public class Queries {

    private Queries() {}

    /**
     * @param pattern a pattern to match in the graph
     */
    public static MatchQueryAdmin match(Conjunction<PatternAdmin> pattern) {
        return new MatchQueryBase(pattern);
    }

    /**
     * @param matchQuery the match query that the ask query will search for in the graph
     */
    public static AskQueryAdmin ask(MatchQuery matchQuery) {
        return new AskQueryImpl(matchQuery);
    }

    /**
     * @param vars a collection of Vars to insert
     * @param matchQuery the match query to insert for each result
     */
    public static InsertQueryAdmin insert(ImmutableCollection<VarAdmin> vars, MatchQueryAdmin matchQuery) {
        return new InsertQueryImpl(vars, Optional.of(matchQuery), Optional.empty());
    }

    /**
     * @param graph the graph to execute on
     * @param vars a collection of Vars to insert
     */
    public static InsertQueryAdmin insert(ImmutableCollection<VarAdmin> vars, Optional<MindmapsGraph> graph) {
        return new InsertQueryImpl(vars, Optional.empty(), graph);
    }

    public static DeleteQueryAdmin delete(Collection<VarAdmin> deleters, MatchQuery matchQuery) {
        return new DeleteQueryImpl(deleters, matchQuery);
    }

    public static ComputeQuery compute(MindmapsGraph graph, String computeMethod) {
        return new ComputeQueryImpl(graph, computeMethod);
    }

    public static ComputeQuery compute(MindmapsGraph graph, String computeMethod, Set<String> typeIds) {
        return new ComputeQueryImpl(graph, computeMethod, typeIds);
    }

    public static <T> AggregateQuery<T> aggregate(MatchQueryAdmin matchQuery, Aggregate<? super Map<String, Concept>, T> aggregate) {
        return new AggregateQueryImpl<>(matchQuery, aggregate);
    }
}
