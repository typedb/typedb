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

import ai.grakn.GraknTx;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Printer;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.InsertQueryAdmin;
import ai.grakn.graql.admin.MatchQueryAdmin;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.property.VarPropertyInternal;
import ai.grakn.util.CommonUtil;
import com.google.common.collect.ImmutableCollection;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.util.CommonUtil.toImmutableList;

/**
 * A query that will insert a collection of variables into a graph
 */
class InsertQueryImpl implements InsertQueryAdmin {

    private final Optional<MatchQueryAdmin> matchQuery;
    private final Optional<GraknTx> graph;
    private final ImmutableCollection<VarPatternAdmin> originalVars;
    private final ImmutableCollection<VarPatternAdmin> vars;

    /**
     * At least one of graph and matchQuery must be absent.
     *
     * @param vars a collection of Vars to insert
     * @param matchQuery the match query to insert for each result
     * @param graph the graph to execute on
     */
    InsertQueryImpl(ImmutableCollection<VarPatternAdmin> vars, Optional<MatchQueryAdmin> matchQuery, Optional<GraknTx> graph) {
        // match query and graph should never both be present (should get graph from inner match query)
        assert(!matchQuery.isPresent() || !graph.isPresent());

        if (vars.isEmpty()) {
            throw GraqlQueryException.noPatterns();
        }

        this.matchQuery = matchQuery;
        this.graph = graph;

        this.originalVars = vars;

        // Get all variables, including ones nested in other variables
        this.vars = vars.stream().flatMap(v -> v.innerVarPatterns().stream()).collect(toImmutableList());

        for (VarPatternAdmin var : this.vars) {
            var.getProperties().forEach(property -> ((VarPropertyInternal) property).checkInsertable(var));
        }
    }

    @Override
    public InsertQuery withGraph(GraknTx graph) {
        return matchQuery.map(
                m -> Queries.insert(vars, m.withGraph(graph).admin())
        ).orElseGet(
                () -> new InsertQueryImpl(vars, Optional.empty(), Optional.of(graph))
        );
    }

    @Override
    public List<Answer> execute() {
        return stream().collect(Collectors.toList());
    }

    @Override
    public Stream<String> resultsString(Printer printer) {
        return stream().map(printer::graqlString);
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public Stream<Answer> stream() {
        GraknTx theGraph = getGraph().orElseThrow(GraqlQueryException::noGraph);

        return matchQuery.map(
                query -> query.stream().map(answer -> InsertQueryExecutor.insertAll(vars, theGraph, answer))
        ).orElseGet(
                () -> Stream.of(InsertQueryExecutor.insertAll(vars, theGraph))
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
    public Set<OntologyConcept> getOntologyConcepts() {
        GraknTx theGraph = getGraph().orElseThrow(GraqlQueryException::noGraph);

        Set<OntologyConcept> types = vars.stream()
                .flatMap(v -> v.innerVarPatterns().stream())
                .map(VarPatternAdmin::getTypeLabel)
                .flatMap(CommonUtil::optionalToStream)
                .map(theGraph::<Type>getOntologyConcept)
                .collect(Collectors.toSet());

        matchQuery.ifPresent(mq -> types.addAll(mq.getOntologyConcepts()));

        return types;
    }

    @Override
    public Collection<VarPatternAdmin> varPatterns() {
        return originalVars;
    }

    @Override
    public Optional<GraknTx> getGraph() {
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
