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
 *
 */

package io.mindmaps.graql.internal.query.match;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.graql.Aggregate;
import io.mindmaps.graql.AggregateQuery;
import io.mindmaps.graql.AskQuery;
import io.mindmaps.graql.DeleteQuery;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.InsertQuery;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.Printer;
import io.mindmaps.graql.Var;
import io.mindmaps.graql.admin.MatchQueryAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.query.Queries;
import io.mindmaps.graql.internal.util.ANSI;
import io.mindmaps.graql.internal.util.AdminConverter;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

@SuppressWarnings("UnusedReturnValue")
public interface MatchQueryInternal extends MatchQueryAdmin {

    /**
     * @param keyword a keyword to color-code using ANSI colors
     * @return the keyword, color-coded
     */
    static String colorKeyword(String keyword) {
        return ANSI.color(keyword, ANSI.BLUE);
    }

    /**
     * @param type a type to color-code using ANSI colors
     * @return the type, color-coded
     */
    static String colorType(String type) {
        return ANSI.color(type, ANSI.PURPLE);
    }

    @Override
    default Stream<String> resultsString(Printer printer) {
        return stream().map(printer::graqlString);
    }

    @Override
    default boolean isReadOnly() {
        return true;
    }

    /**
     * Execute the query using the given graph.
     * @param graph the graph to use to execute the query
     * @param order how to order the resulting stream
     * @return a stream of results
     */
    Stream<Map<String, Concept>> stream(Optional<MindmapsGraph> graph, Optional<MatchOrder> order);

    @Override
    default Stream<Map<String, Concept>> stream() {
        return stream(Optional.empty(), Optional.empty());
    }

    @Override
    default MatchQuery withGraph(MindmapsGraph graph) {
        return new MatchQueryGraph(graph, this);
    }

    @Override
    default MatchQuery limit(long limit) {
        return new MatchQueryLimit(this, limit);
    }

    @Override
    default MatchQuery offset(long offset) {
        return new MatchQueryOffset(this, offset);
    }

    @Override
    default MatchQuery distinct() {
        return new MatchQueryDistinct(this);
    }

    @Override
    default <S> AggregateQuery<S> aggregate(Aggregate<? super Map<String, Concept>, S> aggregate) {
        return Queries.aggregate(admin(), aggregate);
    }

    @Override
    default MatchQuery select(Set<String> names) {
        return new MatchQuerySelect(this, ImmutableSet.copyOf(names));
    }

    @Override
    default Stream<Concept> get(String name) {
        return stream().map(result -> result.get(name));
    }

    @Override
    default AskQuery ask() {
        return Queries.ask(this);
    }

    @Override
    default InsertQuery insert(Collection<? extends Var> vars) {
        ImmutableMultiset<VarAdmin> varAdmins = ImmutableMultiset.copyOf(AdminConverter.getVarAdmins(vars));
        return Queries.insert(varAdmins, admin());
    }

    @Override
    default DeleteQuery delete(String... names) {
        List<Var> deleters = Arrays.stream(names).map(Graql::var).collect(toList());
        return delete(deleters);
    }

    @Override
    default DeleteQuery delete(Collection<? extends Var> deleters) {
        return Queries.delete(AdminConverter.getVarAdmins(deleters), this);
    }

    @Override
    default MatchQuery orderBy(String varName, boolean asc) {
        return new MatchQueryOrder(this, new MatchOrderImpl(varName, asc));
    }
}
