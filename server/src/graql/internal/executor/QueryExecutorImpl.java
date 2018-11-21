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

package grakn.core.graql.internal.executor;

import grakn.core.server.ComputeExecutor;
import grakn.core.server.QueryExecutor;
import grakn.core.graql.concept.Concept;
import grakn.core.server.exception.GraqlQueryException;
import grakn.core.graql.AggregateQuery;
import grakn.core.graql.ComputeQuery;
import grakn.core.graql.DefineQuery;
import grakn.core.graql.DeleteQuery;
import grakn.core.graql.GetQuery;
import grakn.core.graql.InsertQuery;
import grakn.core.graql.Match;
import grakn.core.graql.UndefineQuery;
import grakn.core.graql.Var;
import grakn.core.graql.admin.VarPatternAdmin;
import grakn.core.graql.answer.Answer;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.answer.ConceptSet;
import grakn.core.graql.internal.util.AdminConverter;
import grakn.core.server.session.TransactionImpl;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static grakn.core.common.util.CommonUtil.toImmutableList;
import static grakn.core.common.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * A {@link QueryExecutor} that runs queries using a Tinkerpop graph.
 *
 */
@SuppressWarnings("unused") // accessed via reflection in TransactionImpl
public class QueryExecutorImpl implements QueryExecutor {

    private final TransactionImpl<?> tx;

    private QueryExecutorImpl(TransactionImpl<?> tx) {
        this.tx = tx;
    }

    public static QueryExecutorImpl create(TransactionImpl<?> tx) {
        return new QueryExecutorImpl(tx);
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
        return answers.map(answer -> QueryOperationExecutor.insertAll(varPatterns, tx, answer)).collect(toList()).stream();
    }

    @Override
    public Stream<ConceptSet> run(DeleteQuery query) {
        Stream<ConceptMap> answers = query.admin().match().stream().map(result -> result.project(query.admin().vars())).distinct();
        // TODO: We should not need to collect toSet, once we fix ConceptId.id() to not use cache.
        // Stream.distinct() will then work properly when it calls ConceptImpl.equals()
        Set<Concept> conceptsToDelete = answers.flatMap(answer -> answer.concepts().stream()).collect(toSet());
        conceptsToDelete.forEach(concept -> {
            if (concept.isSchemaConcept()) {
                throw GraqlQueryException.deleteSchemaConcept(concept.asSchemaConcept());
            }
            concept.delete();
        });

        // TODO: return deleted Concepts instead of ConceptIds
        return Stream.of(new ConceptSet(conceptsToDelete.stream().map(Concept::id).collect(toSet())));
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

        ConceptMap undefined = QueryOperationExecutor.undefineAll(allPatterns, tx);
        return Stream.of(undefined);
    }

    @Override
    public <T extends Answer> Stream<T> run(AggregateQuery<T> query) {
        return query.aggregate().apply(query.match().stream()).stream();
    }


    @Override
    public <T extends Answer> ComputeExecutor<T> run(ComputeQuery<T> query) {
        return new ComputeExecutorImpl<>(tx, query);
    }
}
