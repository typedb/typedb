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
import com.google.common.collect.ImmutableSet;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Type;
import io.mindmaps.graql.InsertQuery;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.admin.InsertQueryAdmin;
import io.mindmaps.graql.admin.MatchQueryAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.validation.InsertQueryValidator;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

/**
 * A query that will insert a collection of variables into a graph
 */
class InsertQueryImpl implements InsertQueryAdmin {

    private final Optional<MatchQueryAdmin> matchQuery;
    private final Optional<MindmapsGraph> graph;
    private final ImmutableCollection<VarAdmin> originalVars;
    private final ImmutableCollection<VarAdmin> vars;

    /**
     * At least one of graph and matchQuery must be absent.
     *
     * @param vars a collection of Vars to insert
     * @param matchQuery the match query to insert for each result
     * @param graph the graph to execute on
     */
    InsertQueryImpl(ImmutableCollection<VarAdmin> vars, Optional<MatchQueryAdmin> matchQuery, Optional<MindmapsGraph> graph) {
        // match query and graph should never both be present (should get graph from inner match query)
        assert(!matchQuery.isPresent() || !graph.isPresent());

        this.matchQuery = matchQuery;
        this.graph = graph;

        this.originalVars = vars;

        // Get all variables, including ones nested in other variables
        this.vars = ImmutableSet.copyOf(vars.stream().flatMap(v -> v.getInnerVars().stream()).collect(toSet()));

        getGraph().ifPresent(t -> new InsertQueryValidator(vars).validate(t));
    }

    @Override
    public InsertQuery withGraph(MindmapsGraph graph) {
        return matchQuery.map(
                m -> Queries.insert(vars, m.withGraph(graph).admin())
        ).orElseGet(
                () -> Queries.insert(vars, Optional.of(graph))
        );
    }

    @Override
    public Void execute() {
        // Do nothing, just execute whole stream
        stream().forEach(c -> {});
        return null;
    }

    @Override
    public Stream<String> resultsString() {
        return stream().map(Concept::getId);
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public Stream<Concept> stream() {
        MindmapsGraph theGraph =
                getGraph().orElseThrow(() -> new IllegalStateException(ErrorMessage.NO_GRAPH.getMessage()));

        InsertQueryExecutor executor = new InsertQueryExecutor(vars, theGraph);

        return matchQuery.map(
                query -> query.stream().flatMap(executor::insertAll)
        ).orElseGet(
                executor::insertAll
        );
    }

    @Override
    public InsertQueryAdmin admin() {
        return this;
    }

    @Override
    public Optional<? extends MatchQuery> getMatchQuery() {
        return matchQuery;
    }

    @Override
    public Set<Type> getTypes() {
        MindmapsGraph theGraph =
                getGraph().orElseThrow(() -> new IllegalStateException(ErrorMessage.NO_GRAPH.getMessage()));

        Set<Type> types = vars.stream()
                .flatMap(v -> v.getInnerVars().stream())
                .flatMap(v -> v.getTypeIds().stream())
                .map(theGraph::getType)
                .collect(Collectors.toSet());

        matchQuery.ifPresent(mq -> types.addAll(mq.getTypes()));

        return types;
    }

    @Override
    public Collection<VarAdmin> getVars() {
        return originalVars;
    }

    @Override
    public Optional<MindmapsGraph> getGraph() {
        return matchQuery.map(MatchQueryAdmin::getGraph).orElse(graph);
    }

    @Override
    public String toString() {
        return "insert " + originalVars.stream().map(v -> v + ";").collect(Collectors.joining("\n")).trim();
    }
}
