/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.graql.internal.query.runner;

import ai.grakn.ComputeJob;
import ai.grakn.GraknTx;
import ai.grakn.QueryRunner;
import ai.grakn.concept.Concept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.DefineQuery;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.UndefineQuery;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.analytics.ConnectedComponentQuery;
import ai.grakn.graql.analytics.CorenessQuery;
import ai.grakn.graql.analytics.CountQuery;
import ai.grakn.graql.analytics.DegreeQuery;
import ai.grakn.graql.analytics.KCoreQuery;
import ai.grakn.graql.analytics.MaxQuery;
import ai.grakn.graql.analytics.MeanQuery;
import ai.grakn.graql.analytics.MedianQuery;
import ai.grakn.graql.analytics.MinQuery;
import ai.grakn.graql.analytics.PathQuery;
import ai.grakn.graql.analytics.PathsQuery;
import ai.grakn.graql.analytics.StdQuery;
import ai.grakn.graql.analytics.SumQuery;
import ai.grakn.graql.internal.util.AdminConverter;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.util.CommonUtil.toImmutableList;
import static ai.grakn.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.toList;

/**
 * A {@link QueryRunner} that runs queries using a Tinkerpop graph.
 *
 * @author Felix Chapman
 */
@SuppressWarnings("unused") // accessed via reflection in EmbeddedGraknTx
public class TinkerQueryRunner implements QueryRunner {

    private final GraknTx tx;
    private final TinkerComputeQueryRunner tinkerComputeQueryRunner;

    private TinkerQueryRunner(EmbeddedGraknTx<?> tx) {
        this.tx = tx;
        this.tinkerComputeQueryRunner = TinkerComputeQueryRunner.create(tx);
    }

    public static TinkerQueryRunner create(EmbeddedGraknTx<?> tx) {
        return new TinkerQueryRunner(tx);
    }

    @Override
    public Stream<Answer> run(GetQuery query) {
        return query.match().stream().map(result -> result.project(query.vars())).distinct();
    }

    @Override
    public Stream<Answer> run(InsertQuery query) {
        Collection<VarPatternAdmin> varPatterns = query.admin().varPatterns().stream()
                .flatMap(v -> v.innerVarPatterns().stream())
                .collect(toImmutableList());

        return query.admin().match()
                .map(match -> runMatchInsert(match, varPatterns))
                .orElseGet(() -> Stream.of(QueryOperationExecutor.insertAll(varPatterns, tx)));
    }

    private Stream<Answer> runMatchInsert(Match match, Collection<VarPatternAdmin> varPatterns) {
        Set<Var> varsInMatch = match.admin().getSelectedNames();
        Set<Var> varsInInsert = varPatterns.stream().map(VarPatternAdmin::var).collect(toImmutableSet());
        Set<Var> projectedVars = Sets.intersection(varsInMatch, varsInInsert);

        Stream<Answer> answers = match.get(projectedVars).stream();
        return answers.map(answer -> QueryOperationExecutor.insertAll(varPatterns, tx, answer));
    }

    @Override
    public void run(DeleteQuery query) {
        List<Answer> results = query.admin().match().stream().collect(toList());
        results.forEach(result -> deleteResult(result, query.admin().vars()));
    }

    @Override
    public Answer run(DefineQuery query) {
        ImmutableList<VarPatternAdmin> allPatterns = AdminConverter.getVarAdmins(query.varPatterns()).stream()
                .flatMap(v -> v.innerVarPatterns().stream())
                .collect(toImmutableList());

        return QueryOperationExecutor.defineAll(allPatterns, tx);
    }

    @Override
    public void run(UndefineQuery query) {
        ImmutableList<VarPatternAdmin> allPatterns = AdminConverter.getVarAdmins(query.varPatterns()).stream()
                .flatMap(v -> v.innerVarPatterns().stream())
                .collect(toImmutableList());

        QueryOperationExecutor.undefineAll(allPatterns, tx);
    }

    @Override
    public <T> T run(AggregateQuery<T> query) {
        return query.aggregate().apply(query.match().stream());
    }

    @Override
    public <T> ComputeJob<T> run(ConnectedComponentQuery<T> query) {
        return tinkerComputeQueryRunner.run(query);
    }

    @Override
    public ComputeJob<Map<Long, Set<String>>> run(CorenessQuery query) {
        return tinkerComputeQueryRunner.run(query);
    }

    @Override
    public ComputeJob<Long> run(CountQuery query) {
        return tinkerComputeQueryRunner.run(query);
    }

    @Override
    public ComputeJob<Map<Long, Set<String>>> run(DegreeQuery query) {
        return tinkerComputeQueryRunner.run(query);
    }

    @Override
    public ComputeJob<Map<String, Set<String>>> run(KCoreQuery query) {
        return tinkerComputeQueryRunner.run(query);
    }

    @Override
    public ComputeJob<Optional<Number>> run(MaxQuery query) {
        return tinkerComputeQueryRunner.run(query);
    }

    @Override
    public ComputeJob<Optional<Double>> run(MeanQuery query) {
        return tinkerComputeQueryRunner.run(query);
    }

    @Override
    public ComputeJob<Optional<Number>> run(MedianQuery query) {
        return tinkerComputeQueryRunner.run(query);
    }

    @Override
    public ComputeJob<Optional<Number>> run(MinQuery query) {
        return tinkerComputeQueryRunner.run(query);
    }

    @Override
    public ComputeJob<Optional<List<Concept>>> run(PathQuery query) {
        return tinkerComputeQueryRunner.run(query);
    }

    @Override
    public ComputeJob<List<List<Concept>>> run(PathsQuery query) {
        return tinkerComputeQueryRunner.run(query);
    }

    @Override
    public ComputeJob<Optional<Double>> run(StdQuery query) {
        return tinkerComputeQueryRunner.run(query);
    }

    @Override
    public ComputeJob<Optional<Number>> run(SumQuery query) {
        return tinkerComputeQueryRunner.run(query);
    }

    private void deleteResult(Answer result, Collection<? extends Var> vars) {
        Collection<? extends Var> toDelete = vars.isEmpty() ? result.vars() : vars;

        for (Var var : toDelete) {
            Concept concept = result.get(var);

            if (concept.isSchemaConcept()) {
                throw GraqlQueryException.deleteSchemaConcept(concept.asSchemaConcept());
            }

            concept.delete();
        }
    }
}
