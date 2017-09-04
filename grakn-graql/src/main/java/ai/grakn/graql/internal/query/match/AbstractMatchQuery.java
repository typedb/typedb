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

package ai.grakn.graql.internal.query.match;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Aggregate;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Order;
import ai.grakn.graql.Printer;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.MatchQueryAdmin;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.query.Queries;
import ai.grakn.graql.internal.util.AdminConverter;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.Order.asc;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

@SuppressWarnings("UnusedReturnValue")
abstract class AbstractMatchQuery implements MatchQueryAdmin {

    @Override
    public final Stream<String> resultsString(Printer printer) {
        return stream().map(printer::graqlString);
    }

    @Override
    public final boolean isReadOnly() {
        return true;
    }

    @Override
    public final MatchQueryAdmin admin() {
        return this;
    }

    @Override
    public final List<Answer> execute() {
        return stream().collect(toList());
    }

    /**
     * Execute the query using the given graph.
     * @param graph the graph to use to execute the query
     * @return a stream of results
     */
    public abstract Stream<Answer> stream(Optional<GraknTx> graph);

    @Override
    public final Stream<Answer> stream() {
        return stream(Optional.empty());
    }

    @Override
    public final MatchQuery withTx(GraknTx tx) {
        return new MatchQueryTx(tx, this);
    }

    @Override
    public final MatchQuery limit(long limit) {
        return new MatchQueryLimit(this, limit);
    }

    @Override
    public final MatchQuery offset(long offset) {
        return new MatchQueryOffset(this, offset);
    }

    @Override
    public final MatchQuery distinct() {
        return new MatchQueryDistinct(this);
    }

    @Override
    public final <S> AggregateQuery<S> aggregate(Aggregate<? super Answer, S> aggregate) {
        return Queries.aggregate(admin(), aggregate);
    }

    @Override
    public final MatchQuery select(String... vars) {
        return select(Stream.of(vars).map(Graql::var).collect(toSet()));
    }

    @Override
    public final MatchQuery select(Var... vars) {
        return select(Sets.newHashSet(vars));
    }

    @Override
    public final MatchQuery select(Set<Var> vars) {
        return new MatchQuerySelect(this, ImmutableSet.copyOf(vars));
    }

    @Override
    public final Stream<Concept> get(String var) {
        return get(Graql.var(var));
    }

    @Override
    public final Stream<Concept> get(Var var) {
        return stream().map(result -> {
            if (!result.containsKey(var)) {
                throw GraqlQueryException.varNotInQuery(var);
            }
            return result.get(var);
        });
    }

    @Override
    public final InsertQuery insert(VarPattern... vars) {
        return insert(Arrays.asList(vars));
    }

    @Override
    public final InsertQuery insert(Collection<? extends VarPattern> vars) {
        ImmutableMultiset<VarPatternAdmin> varAdmins = ImmutableMultiset.copyOf(AdminConverter.getVarAdmins(vars));
        return Queries.insert(varAdmins, admin());
    }

    @Override
    public final DeleteQuery delete(String var, String... vars) {
        List<Var> varList = Stream.concat(Stream.of(var), Arrays.stream(vars)).map(Graql::var).collect(toList());
        return delete(varList);
    }

    @Override
    public final DeleteQuery delete(Var... vars) {
        return delete(Arrays.asList(vars));
    }

    @Override
    public final DeleteQuery delete(Collection<? extends Var> vars) {
        return Queries.delete(vars, this);
    }

    @Override
    public final MatchQuery orderBy(String varName) {
        return orderBy(varName, asc);
    }

    @Override
    public final MatchQuery orderBy(Var varName) {
        return orderBy(varName, asc);
    }

    @Override
    public final MatchQuery orderBy(String varName, Order order) {
        return orderBy(Graql.var(varName), order);
    }

    @Override
    public final MatchQuery orderBy(Var varName, Order order) {
        return new MatchQueryOrder(this, new MatchOrderImpl(varName, order));
    }
}
