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

package ai.grakn.graql.internal.query.match;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Aggregate;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.Order;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.MatchAdmin;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.property.VarPropertyInternal;
import ai.grakn.graql.internal.query.Queries;
import ai.grakn.graql.internal.util.AdminConverter;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.Order.asc;
import static ai.grakn.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.toList;

@SuppressWarnings("UnusedReturnValue")
abstract class AbstractMatch implements MatchAdmin {

    @Override
    public final MatchAdmin admin() {
        return this;
    }

    /**
     * Execute the query using the given graph.
     * @param tx the graph to use to execute the query
     * @return a stream of results
     */
    public abstract Stream<Answer> stream(EmbeddedGraknTx<?> tx);

    @Override
    public final Stream<Answer> stream() {
        return stream(null);
    }

    /**
     * @param tx the {@link GraknTx} against which the pattern should be validated
     */
    void validatePattern(GraknTx tx){
        for (VarPatternAdmin var : getPattern().varPatterns()) {
            var.getProperties().forEach(property -> ((VarPropertyInternal) property).checkValid(tx, var));}
    }

    @Override
    public final Match withTx(GraknTx tx) {
        return new MatchTx(tx, this);
    }

    @Override
    public final Match limit(long limit) {
        return new MatchLimit(this, limit);
    }

    @Override
    public final Match offset(long offset) {
        return new MatchOffset(this, offset);
    }

    @Override
    public final <S> AggregateQuery<S> aggregate(Aggregate<? super Answer, S> aggregate) {
        return Queries.aggregate(admin(), aggregate);
    }

    @Override
    public final Stream<Concept> get(String var) {
        return get(Graql.var(var));
    }

    @Override
    public final Stream<Concept> get(Var var) {
        return stream().map(result -> {
            if (!result.containsVar(var)) {
                throw GraqlQueryException.varNotInQuery(var);
            }
            return result.get(var);
        });
    }

    @Override
    public GetQuery get() {
        return get(getPattern().commonVars());
    }

    @Override
    public GetQuery get(String var, String... vars) {
        Stream<String> varStream = Stream.concat(Stream.of(var), Stream.of(vars));
        return get(varStream.map(Graql::var).collect(toImmutableSet()));
    }

    @Override
    public GetQuery get(Var var, Var... vars) {
        Stream<Var> varStream = Stream.concat(Stream.of(var), Stream.of(vars));
        return get(varStream.collect(toImmutableSet()));
    }

    @Override
    public GetQuery get(Set<Var> vars) {
        return Queries.get(ImmutableSet.copyOf(vars), this);
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
    public final Match orderBy(String varName) {
        return orderBy(varName, asc);
    }

    @Override
    public final Match orderBy(Var varName) {
        return orderBy(varName, asc);
    }

    @Override
    public final Match orderBy(String varName, Order order) {
        return orderBy(Graql.var(varName), order);
    }

    @Override
    public final Match orderBy(Var varName, Order order) {
        return new MatchOrder(this, Ordering.of(varName, order));
    }
}
