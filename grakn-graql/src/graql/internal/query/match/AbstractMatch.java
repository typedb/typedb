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

package grakn.core.graql.internal.query.match;

import grakn.core.GraknTx;
import grakn.core.graql.Aggregate;
import grakn.core.graql.AggregateQuery;
import grakn.core.graql.DeleteQuery;
import grakn.core.graql.GetQuery;
import grakn.core.graql.Graql;
import grakn.core.graql.InsertQuery;
import grakn.core.graql.Match;
import grakn.core.graql.Order;
import grakn.core.graql.Var;
import grakn.core.graql.VarPattern;
import grakn.core.graql.answer.Answer;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.admin.MatchAdmin;
import grakn.core.graql.admin.VarPatternAdmin;
import grakn.core.graql.internal.pattern.property.VarPropertyInternal;
import grakn.core.graql.internal.query.Queries;
import grakn.core.graql.internal.util.AdminConverter;
import grakn.core.kb.internal.EmbeddedGraknTx;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.graql.Order.asc;

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
    public abstract Stream<ConceptMap> stream(EmbeddedGraknTx<?> tx);

    @Override
    public final Stream<ConceptMap> stream() {
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
    public final <S extends Answer> AggregateQuery<S> aggregate(Aggregate<S> aggregate) {
        return Queries.aggregate(admin(), aggregate);
    }

    @Override
    public GetQuery get() {
        return get(getPattern().commonVars());
    }

    @Override
    public GetQuery get(String var, String... vars) {
        Set<Var> varSet = Stream.concat(Stream.of(var), Stream.of(vars)).map(Graql::var).collect(Collectors.toSet());
        return get(varSet);
    }

    @Override
    public GetQuery get(Var var, Var... vars) {
        Set<Var> varSet = new HashSet<>(Arrays.asList(vars));
        varSet.add(var);
        return get(varSet);
    }

    @Override
    public GetQuery get(Set<Var> vars) {
        if (vars.isEmpty()) vars = getPattern().commonVars();
        return Queries.get(this, ImmutableSet.copyOf(vars));
    }

    @Override
    public final InsertQuery insert(VarPattern... vars) {
        return insert(Arrays.asList(vars));
    }

    @Override
    public final InsertQuery insert(Collection<? extends VarPattern> vars) {
        ImmutableMultiset<VarPatternAdmin> varAdmins = ImmutableMultiset.copyOf(AdminConverter.getVarAdmins(vars));
        return Queries.insert(admin(), varAdmins);
    }

    @Override
    public DeleteQuery delete() {
        return delete(getPattern().commonVars());
    }

    @Override
    public final DeleteQuery delete(String var, String... vars) {
        Set<Var> varSet = Stream.concat(Stream.of(var), Arrays.stream(vars)).map(Graql::var).collect(Collectors.toSet());
        return delete(varSet);
    }

    @Override
    public final DeleteQuery delete(Var var, Var... vars) {
        Set<Var> varSet = new HashSet<>(Arrays.asList(vars));
        varSet.add(var);
        return delete(varSet);
    }

    @Override
    public final DeleteQuery delete(Set<Var> vars) {
        if (vars.isEmpty()) vars = getPattern().commonVars();
        return Queries.delete(this, vars);
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
