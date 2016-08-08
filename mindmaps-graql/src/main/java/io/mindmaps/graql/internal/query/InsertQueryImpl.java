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
import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.core.model.Concept;
import io.mindmaps.graql.api.query.InsertQuery;
import io.mindmaps.graql.api.query.MatchQuery;
import io.mindmaps.graql.api.query.Var;
import io.mindmaps.graql.internal.validation.ErrorMessage;
import io.mindmaps.graql.internal.validation.InsertQueryValidator;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

/**
 * A query that will insert a collection of variables into a graph
 */
public class InsertQueryImpl implements InsertQuery.Admin {

    private final Optional<MatchQuery> matchQuery;
    private final Optional<MindmapsTransaction> transaction;
    private final ImmutableCollection<Var.Admin> originalVars;
    private final ImmutableCollection<Var.Admin> vars;

    /**
     * @param transaction the transaction to execute on
     * @param vars a collection of Vars to insert
     * @param matchQuery the match query to insert for each result
     */
    public InsertQueryImpl(ImmutableCollection<Var.Admin> vars, Optional<MatchQuery> matchQuery, Optional<MindmapsTransaction> transaction) {
        this.transaction = transaction;

        this.originalVars = vars;
        this.matchQuery = matchQuery;

        // Get all variables, including ones nested in other variables
        this.vars = ImmutableSet.copyOf(vars.stream().flatMap(v -> v.getInnerVars().stream()).collect(toSet()));


        transaction.ifPresent(t -> new InsertQueryValidator(this).validate(t));
    }

    @Override
    public InsertQuery withTransaction(MindmapsTransaction transaction) {
        Optional<MatchQuery> newMatchQuery = matchQuery.map(m -> m.withTransaction(transaction));
        return new InsertQueryImpl(vars, newMatchQuery, Optional.of(transaction));
    }

    @Override
    public void execute() {
        // Do nothing, just execute whole stream
        stream().forEach(c -> {});
    }

    @Override
    public Stream<Concept> stream() {
        MindmapsTransaction theTransaction =
                transaction.orElseThrow(() -> new IllegalStateException(ErrorMessage.NO_TRANSACTION.getMessage()));

        InsertQueryExecutor executor = new InsertQueryExecutor(vars, theTransaction);

        return matchQuery.map(
                query -> query.stream().flatMap(executor::insertAll)
        ).orElseGet(
                executor::insertAll
        );
    }

    @Override
    public Admin admin() {
        return this;
    }

    @Override
    public Optional<MatchQuery> getMatchQuery() {
        return matchQuery;
    }

    @Override
    public Collection<Var.Admin> getVars() {
        return originalVars;
    }

    @Override
    public Collection<Var.Admin> getAllVars() {
        return vars;
    }

    @Override
    public Optional<MindmapsTransaction> getTransaction() {
        return transaction;
    }

    @Override
    public String toString() {
        return "insert " + originalVars.stream().map(v -> v + ";").collect(Collectors.joining("\n")).trim();
    }
}
