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

import com.google.common.collect.ImmutableSet;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.core.model.Concept;
import io.mindmaps.graql.*;
import io.mindmaps.graql.admin.AdminConverter;
import io.mindmaps.graql.admin.MatchQueryAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.query.AskQueryImpl;
import io.mindmaps.graql.internal.query.DeleteQueryImpl;
import io.mindmaps.graql.internal.query.InsertQueryImpl;
import io.mindmaps.graql.internal.query.aggregate.AggregateQueryImpl;

import java.util.*;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

@SuppressWarnings("UnusedReturnValue")
abstract class AbstractMatchQuery implements MatchQueryAdmin {

    @Override
    public MatchQuery withGraph(MindmapsGraph graph) {
        return new MatchQueryGraph(graph, admin());
    }

    @Override
    public MatchQuery limit(long limit) {
        return new MatchQueryLimit(admin(), limit);
    }

    @Override
    public MatchQuery offset(long offset) {
        return new MatchQueryOffset(admin(), offset);
    }

    @Override
    public MatchQuery distinct() {
        return new MatchQueryDistinct(admin());
    }

    @Override
    public final <S> AggregateQuery<S> aggregate(Aggregate<? super Map<String, Concept>, S> aggregate) {
        return new AggregateQueryImpl<>(admin(), aggregate);
    }

    @Override
    public final MatchQuery select(Set<String> names) {
        return new MatchQuerySelect(admin(), ImmutableSet.copyOf(names));
    }

    @Override
    public final Stream<Concept> get(String name) {
        return stream().map(result -> result.get(name));
    }

    @Override
    public final AskQuery ask() {
        return new AskQueryImpl(this);
    }

    @Override
    public final InsertQuery insert(Collection<? extends Var> vars) {
        ImmutableSet<VarAdmin> varAdmins = ImmutableSet.copyOf(AdminConverter.getVarAdmins(vars));
        return new InsertQueryImpl(varAdmins, admin());
    }

    @Override
    public final DeleteQuery delete(String... names) {
        List<Var> deleters = Arrays.stream(names).map(Graql::var).collect(toList());
        return delete(deleters);
    }

    @Override
    public final DeleteQuery delete(Collection<? extends Var> deleters) {
        return new DeleteQueryImpl(AdminConverter.getVarAdmins(deleters), this);
    }

    @Override
    public final MatchQuery orderBy(String varName, boolean asc) {
        return new MatchQueryOrder(admin(), new MatchOrderImpl(varName, Optional.empty(), asc));
    }

    @Override
    public final MatchQuery orderBy(String varName, String resourceType, boolean asc) {
        return new MatchQueryOrder(admin(), new MatchOrderImpl(varName, Optional.of(resourceType), asc));
    }
}
