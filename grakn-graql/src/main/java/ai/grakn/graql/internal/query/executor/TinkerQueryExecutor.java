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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.query.executor;

import ai.grakn.ComputeExecutor;
import ai.grakn.QueryExecutor;
import ai.grakn.concept.Concept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.DefineQuery;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.UndefineQuery;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.answer.Answer;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.internal.util.AdminConverter;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.util.CommonUtil.toImmutableList;
import static ai.grakn.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.toList;

/**
 * A {@link QueryExecutor} that runs queries using a Tinkerpop graph.
 *
 * @author Grakn Warriors
 */
@SuppressWarnings("unused") // accessed via reflection in EmbeddedGraknTx
public class TinkerQueryExecutor implements QueryExecutor {

    private final EmbeddedGraknTx<?> tx;

    private TinkerQueryExecutor(EmbeddedGraknTx<?> tx) {
        this.tx = tx;
    }

    public static TinkerQueryExecutor create(EmbeddedGraknTx<?> tx) {
        return new TinkerQueryExecutor(tx);
    }

    @Override
    public Stream<ConceptMap> run(GetQuery query) {
        return query.match().stream().map(result -> result.project(query.vars())).distinct();
    }

    @Override
    public Stream<ConceptMap> run(InsertQuery query) {
        Collection<VarPatternAdmin> varPatterns = query.admin().varPatterns().stream()
                .flatMap(v -> v.innerVarPatterns().stream())
                .collect(toImmutableList());

        if (query.admin().match() != null) {
            return runMatchInsert(query.admin().match(), varPatterns);
        } else {
            return Stream.of(QueryOperationExecutor.insertAll(varPatterns, tx));
        }
    }

    private Stream<ConceptMap> runMatchInsert(Match match, Collection<VarPatternAdmin> varPatterns) {
        Set<Var> varsInMatch = match.admin().getSelectedNames();
        Set<Var> varsInInsert = varPatterns.stream().map(VarPatternAdmin::var).collect(toImmutableSet());
        Set<Var> projectedVars = Sets.intersection(varsInMatch, varsInInsert);

        Stream<ConceptMap> answers = match.get(projectedVars).stream();
        return answers.map(answer -> QueryOperationExecutor.insertAll(varPatterns, tx, answer));
    }

    @Override
    public Stream<ConceptMap> run(DeleteQuery query) {
        List<ConceptMap> results = query.admin().match().stream().collect(toList());
        results.forEach(result -> deleteResult(result, query.admin().vars()));

        // TODO: return deleted concepts
        return Stream.empty();
    }

    @Override
    public Stream<ConceptMap> run(DefineQuery query) {
        ImmutableList<VarPatternAdmin> allPatterns = AdminConverter.getVarAdmins(query.varPatterns()).stream()
                .flatMap(v -> v.innerVarPatterns().stream())
                .collect(toImmutableList());

        ConceptMap defined = QueryOperationExecutor.defineAll(allPatterns, tx);
        return Stream.of(defined);
    }

    @Override
    public Stream<ConceptMap> run(UndefineQuery query) {
        ImmutableList<VarPatternAdmin> allPatterns = AdminConverter.getVarAdmins(query.varPatterns()).stream()
                .flatMap(v -> v.innerVarPatterns().stream())
                .collect(toImmutableList());

        QueryOperationExecutor.undefineAll(allPatterns, tx);

        // TODO: Return undefined concepts
        return Stream.empty();
    }

    @Override
    public <T extends Answer> List<T> run(AggregateQuery<T> query) {
        return query.aggregate().apply(query.match().stream());
    }


    @Override
    public <T extends Answer> ComputeExecutor<T> run(ComputeQuery<T> query) {
        return new TinkerComputeExecutor<>(tx, query);
    }

    private void deleteResult(ConceptMap result, Collection<? extends Var> vars) {
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
