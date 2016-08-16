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
import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.InsertQuery;
import io.mindmaps.graql.MatchQueryDefault;
import io.mindmaps.graql.internal.admin.InsertQueryAdmin;
import io.mindmaps.graql.internal.admin.MatchQueryDefaultAdmin;
import io.mindmaps.graql.internal.admin.VarAdmin;
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
public class InsertQueryImpl implements InsertQueryAdmin {

    private final Optional<MatchQueryDefaultAdmin> matchQuery;
    private final Optional<MindmapsTransaction> transaction;
    private final ImmutableCollection<VarAdmin> originalVars;
    private final ImmutableCollection<VarAdmin> vars;

    /**
     * At least one of transaction and matchQuery must be absent.
     *
     * @param vars a collection of Vars to insert
     * @param matchQuery the match query to insert for each result
     * @param transaction the transaction to execute on
     */
    private InsertQueryImpl(ImmutableCollection<VarAdmin> vars, Optional<MatchQueryDefaultAdmin> matchQuery, Optional<MindmapsTransaction> transaction) {
        // match query and transaction should never both be present (should get transaction from inner match query)
        assert(!matchQuery.isPresent() || !transaction.isPresent());

        this.matchQuery = matchQuery;
        this.transaction = transaction;

        this.originalVars = vars;

        // Get all variables, including ones nested in other variables
        this.vars = ImmutableSet.copyOf(vars.stream().flatMap(v -> v.getInnerVars().stream()).collect(toSet()));

        getTransaction().ifPresent(t -> new InsertQueryValidator(this).validate(t));
    }

    /**
     * @param vars a collection of Vars to insert
     * @param matchQuery the match query to insert for each result
     */
    public InsertQueryImpl(ImmutableCollection<VarAdmin> vars, MatchQueryDefaultAdmin matchQuery) {
        this(vars, Optional.of(matchQuery), Optional.empty());
    }

    /**
     * @param transaction the transaction to execute on
     * @param vars a collection of Vars to insert
     */
    public InsertQueryImpl(ImmutableCollection<VarAdmin> vars, Optional<MindmapsTransaction> transaction) {
        this(vars, Optional.empty(), transaction);
    }

    @Override
    public InsertQuery withTransaction(MindmapsTransaction transaction) {
        return matchQuery.map(
                m -> new InsertQueryImpl(vars, m.withTransaction(transaction).admin())
        ).orElseGet(
                () -> new InsertQueryImpl(vars, Optional.of(transaction))
        );
    }

    @Override
    public void execute() {
        // Do nothing, just execute whole stream
        stream().forEach(c -> {});
    }

    @Override
    public Stream<Concept> stream() {
        MindmapsTransaction theTransaction =
                getTransaction().orElseThrow(() -> new IllegalStateException(ErrorMessage.NO_TRANSACTION.getMessage()));

        InsertQueryExecutor executor = new InsertQueryExecutor(vars, theTransaction);

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
    public Optional<? extends MatchQueryDefault> getMatchQuery() {
        return matchQuery;
    }

    @Override
    public Set<Type> getTypes() {
        MindmapsTransaction theTransaction =
                getTransaction().orElseThrow(() -> new IllegalStateException(ErrorMessage.NO_TRANSACTION.getMessage()));

        Set<Type> types = vars.stream()
                .flatMap(v -> v.getInnerVars().stream())
                .flatMap(v -> v.getTypeIds().stream())
                .map(theTransaction::getType)
                .collect(Collectors.toSet());

        matchQuery.ifPresent(mq -> types.addAll(mq.getTypes()));

        return types;
    }

    @Override
    public Collection<VarAdmin> getVars() {
        return originalVars;
    }

    @Override
    public Collection<VarAdmin> getAllVars() {
        return vars;
    }

    @Override
    public Optional<MindmapsTransaction> getTransaction() {
        return matchQuery.map(MatchQueryDefaultAdmin::getTransaction).orElse(transaction);
    }

    @Override
    public String toString() {
        return "insert " + originalVars.stream().map(v -> v + ";").collect(Collectors.joining("\n")).trim();
    }
}
