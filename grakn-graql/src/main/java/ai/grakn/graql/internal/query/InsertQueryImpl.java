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
import ai.grakn.concept.Concept;
import ai.grakn.concept.Type;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Printer;
import ai.grakn.graql.admin.InsertQueryAdmin;
import ai.grakn.graql.admin.MatchQueryAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.pattern.property.VarPropertyInternal;
import ai.grakn.graql.internal.util.CommonUtil;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.ImmutableCollection;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.util.CommonUtil.toImmutableList;
import static ai.grakn.util.ErrorMessage.NO_PATTERNS;

/**
 * A query that will insert a collection of variables into a graph
 */
class InsertQueryImpl implements InsertQueryAdmin {

    private final Optional<MatchQueryAdmin> matchQuery;
    private final Optional<GraknGraph> graph;
    private final ImmutableCollection<VarAdmin> originalVars;
    private final ImmutableCollection<VarAdmin> vars;

    /**
     * At least one of graph and matchQuery must be absent.
     *
     * @param vars a collection of Vars to insert
     * @param matchQuery the match query to insert for each result
     * @param graph the graph to execute on
     */
    InsertQueryImpl(ImmutableCollection<VarAdmin> vars, Optional<MatchQueryAdmin> matchQuery, Optional<GraknGraph> graph) {
        // match query and graph should never both be present (should get graph from inner match query)
        assert(!matchQuery.isPresent() || !graph.isPresent());

        if (vars.isEmpty()) {
            throw new IllegalArgumentException(NO_PATTERNS.getMessage());
        }

        this.matchQuery = matchQuery;
        this.graph = graph;

        this.originalVars = vars;

        // Get all variables, including ones nested in other variables
        this.vars = vars.stream().flatMap(v -> v.getInnerVars().stream()).collect(toImmutableList());

        for (VarAdmin var : this.vars) {
            var.getProperties().forEach(property -> ((VarPropertyInternal) property).checkInsertable(var));
        }
    }

    @Override
    public InsertQuery withGraph(GraknGraph graph) {
        return matchQuery.map(
                m -> Queries.insert(vars, m.withGraph(graph).admin())
        ).orElseGet(
                () -> new InsertQueryImpl(vars, Optional.empty(), Optional.of(graph))
        );
    }

    @Override
    public List<Map<String, Concept>> execute() {
        return stream().collect(Collectors.toList());
    }

    @Override
    public Stream<String> resultsString(Printer printer) {
        return streamWithVarNames().map(printer::graqlString);
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public Stream<Map<String, Concept>> stream() {
        return streamWithVarNames().map(CommonUtil::resultVarNameToString);
    }

    @Override
    public Stream<Map<VarName, Concept>> streamWithVarNames() {
        GraknGraph theGraph =
                getGraph().orElseThrow(() -> new IllegalStateException(ErrorMessage.NO_GRAPH.getMessage()));

        InsertQueryExecutor executor = new InsertQueryExecutor(vars, theGraph);

        return matchQuery.map(
                query -> query.streamWithVarNames().map(executor::insertAll)
        ).orElseGet(
                () -> Stream.of(executor.insertAll())
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
        GraknGraph theGraph =
                getGraph().orElseThrow(() -> new IllegalStateException(ErrorMessage.NO_GRAPH.getMessage()));

        Set<Type> types = vars.stream()
                .flatMap(v -> v.getInnerVars().stream())
                .map(VarAdmin::getTypeLabel)
                .flatMap(CommonUtil::optionalToStream)
                .map(theGraph::<Type>getType)
                .collect(Collectors.toSet());

        matchQuery.ifPresent(mq -> types.addAll(mq.getTypes()));

        return types;
    }

    @Override
    public Collection<VarAdmin> getVars() {
        return originalVars;
    }

    @Override
    public Optional<GraknGraph> getGraph() {
        return matchQuery.map(MatchQueryAdmin::getGraph).orElse(graph);
    }

    @Override
    public String toString() {
        String mq = matchQuery.map(match -> match + "\n").orElse("");
        return mq + "insert " + originalVars.stream().map(v -> v + ";").collect(Collectors.joining("\n")).trim();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InsertQueryImpl maps = (InsertQueryImpl) o;

        if (!matchQuery.equals(maps.matchQuery)) return false;
        if (!graph.equals(maps.graph)) return false;
        return originalVars.equals(maps.originalVars);
    }

    @Override
    public int hashCode() {
        int result = matchQuery.hashCode();
        result = 31 * result + graph.hashCode();
        result = 31 * result + originalVars.hashCode();
        return result;
    }
}
