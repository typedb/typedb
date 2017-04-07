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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.graql.Aggregate;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.AskQuery;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Order;
import ai.grakn.graql.Printer;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.MatchQueryAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.query.Queries;
import ai.grakn.graql.internal.util.AdminConverter;
import ai.grakn.graql.internal.util.CommonUtil;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.Order.asc;
import static ai.grakn.util.ErrorMessage.VARIABLE_NOT_IN_QUERY;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

@SuppressWarnings("UnusedReturnValue")
abstract class AbstractMatchQuery implements MatchQueryAdmin {

    @Override
    public final Stream<String> resultsString(Printer printer) {
        return streamWithVarNames().map(printer::graqlString);
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
    public final List<Map<String, Concept>> execute() {
        return stream().collect(toList());
    }

    @Override
    public final List<Map<VarName, Concept>> results() {
        return streamWithVarNames().collect(toList());
    }

    /**
     * Execute the query using the given graph.
     * @param graph the graph to use to execute the query
     * @return a stream of results
     */
    public abstract Stream<Answer> stream(Optional<GraknGraph> graph);

    @Override
    public final Stream<Map<VarName, Concept>> streamWithVarNames() {
        return stream(Optional.empty()).map(Answer::map);
    }

    @Override
    public final Stream<Answer> streamWithAnswers(){
        return stream(Optional.empty());
    }

    @Override
    public final Stream<Map<String, Concept>> stream() {
        return streamWithVarNames().map(CommonUtil::resultVarNameToString);
    }

    @Override
    public final MatchQuery withGraph(GraknGraph graph) {
        return new MatchQueryGraph(graph, this);
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
    public final <S> AggregateQuery<S> aggregate(Aggregate<? super Map<VarName, Concept>, S> aggregate) {
        return Queries.aggregate(admin(), aggregate);
    }

    @Override
    public final MatchQuery select(String... names) {
        return select(Stream.of(names).map(VarName::of).collect(toSet()));
    }

    @Override
    public final MatchQuery select(Set<VarName> names) {
        return new MatchQuerySelect(this, ImmutableSet.copyOf(names));
    }

    @Override
    public final Stream<Concept> get(String name) {
        return stream().map(result -> {
            if (!result.containsKey(name)) {
                throw new IllegalArgumentException(VARIABLE_NOT_IN_QUERY.getMessage(VarName.of(name)));
            }
            return result.get(name);
        });
    }

    @Override
    public final AskQuery ask() {
        return Queries.ask(this);
    }

    @Override
    public final InsertQuery insert(Var... vars) {
        return insert(Arrays.asList(vars));
    }

    @Override
    public final InsertQuery insert(Collection<? extends Var> vars) {
        ImmutableMultiset<VarAdmin> varAdmins = ImmutableMultiset.copyOf(AdminConverter.getVarAdmins(vars));
        return Queries.insert(varAdmins, admin());
    }

    @Override
    public final DeleteQuery delete(Var... deleters) {
        return delete(Arrays.asList(deleters));
    }

    @Override
    public final DeleteQuery delete(String... names) {
        List<Var> deleters = Arrays.stream(names).map(Graql::var).collect(toList());
        return delete(deleters);
    }

    @Override
    public final DeleteQuery delete(Collection<? extends Var> deleters) {
        return Queries.delete(AdminConverter.getVarAdmins(deleters), this);
    }

    @Override
    public final MatchQuery orderBy(String varName) {
        return orderBy(varName, asc);
    }

    @Override
    public final MatchQuery orderBy(VarName varName) {
        return orderBy(varName, asc);
    }

    @Override
    public final MatchQuery orderBy(String varName, Order order) {
        return orderBy(VarName.of(varName), order);
    }

    @Override
    public final MatchQuery orderBy(VarName varName, Order order) {
        return new MatchQueryOrder(this, new MatchOrderImpl(varName, order));
    }
}
