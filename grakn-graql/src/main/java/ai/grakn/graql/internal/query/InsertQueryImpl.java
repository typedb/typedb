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

package ai.grakn.graql.internal.query;

import ai.grakn.GraknTx;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.GraqlConverter;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.InsertQueryAdmin;
import ai.grakn.graql.admin.MatchAdmin;
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

    private final Optional<MatchAdmin> match;
    private final Optional<GraknTx> tx;
    private final ImmutableCollection<VarPatternAdmin> originalVars;
    private final ImmutableCollection<VarPatternAdmin> vars;

    /**
     * At least one of {@code tx} and {@code match} must be absent.
     *
     * @param vars a collection of Vars to insert
     * @param match the {@link Match} to insert for each result
     * @param tx the graph to execute on
     */
    InsertQueryImpl(ImmutableCollection<VarPatternAdmin> vars, Optional<MatchAdmin> match, Optional<GraknTx> tx) {
        // match and graph should never both be present (should get graph from inner match)
        assert(!match.isPresent() || !tx.isPresent());

        if (vars.isEmpty()) {
            throw GraqlQueryException.noPatterns();
        }

        this.match = match;
        this.tx = tx;

        this.originalVars = vars;

        // Get all variables, including ones nested in other variables
        this.vars = vars.stream().flatMap(v -> v.innerVarPatterns().stream()).collect(toImmutableList());

        for (VarPatternAdmin var : this.vars) {
            var.getProperties().forEach(property -> ((VarPropertyInternal) property).checkInsertable(var));
        }
    }

    @Override
    public InsertQuery withTx(GraknTx tx) {
        return match.map(
                m -> Queries.insert(vars, m.withTx(tx).admin())
        ).orElseGet(
                () -> new InsertQueryImpl(vars, Optional.empty(), Optional.of(tx))
        );
    }

    @Override
    public List<Answer> execute() {
        return stream().collect(Collectors.toList());
    }

    @Override
    public <T> Stream<T> results(GraqlConverter<?, T> converter) {
        return stream().map(converter::convert);
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public Stream<Answer> stream() {
        GraknTx theGraph = getTx().orElseThrow(GraqlQueryException::noTx);

        return match.map(
                query -> query.stream().map(answer -> QueryOperationExecutor.insertAll(vars, theGraph, answer))
        ).orElseGet(
                () -> Stream.of(QueryOperationExecutor.insertAll(vars, theGraph))
        );
    }

    @Override
    public InsertQueryAdmin admin() {
        return this;
    }

    @Override
    public Optional<? extends Match> match() {
        return match;
    }

    @Override
    public Set<SchemaConcept> getSchemaConcepts() {
        GraknTx theGraph = getTx().orElseThrow(GraqlQueryException::noTx);

        Set<SchemaConcept> types = vars.stream()
                .flatMap(v -> v.innerVarPatterns().stream())
                .map(VarPatternAdmin::getTypeLabel)
                .flatMap(CommonUtil::optionalToStream)
                .map(theGraph::<Type>getSchemaConcept)
                .collect(Collectors.toSet());

        match.ifPresent(mq -> types.addAll(mq.getSchemaConcepts()));

        return types;
    }

    @Override
    public Collection<VarPatternAdmin> varPatterns() {
        return originalVars;
    }

    @Override
    public Optional<GraknTx> getTx() {
        return match.map(MatchAdmin::tx).orElse(tx);
    }

    @Override
    public String toString() {
        String mq = match.map(match -> match + "\n").orElse("");
        return mq + "insert " + originalVars.stream().map(v -> v + ";").collect(Collectors.joining("\n")).trim();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InsertQueryImpl maps = (InsertQueryImpl) o;

        if (!match.equals(maps.match)) return false;
        if (!tx.equals(maps.tx)) return false;
        return originalVars.equals(maps.originalVars);
    }

    @Override
    public int hashCode() {
        int result = match.hashCode();
        result = 31 * result + tx.hashCode();
        result = 31 * result + originalVars.hashCode();
        return result;
    }
}
