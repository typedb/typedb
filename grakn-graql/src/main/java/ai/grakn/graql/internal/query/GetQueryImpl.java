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
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.GraqlConverter;
import ai.grakn.graql.Match;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * Default implementation of {@link GetQuery}
 *
 * @author Felix Chapman
 */
@AutoValue
public abstract class GetQueryImpl implements GetQuery {

    public abstract ImmutableSet<Var> vars();
    public abstract Match match();

    @Override
    public GetQuery withTx(GraknTx tx) {
        return Queries.get(vars(), match().withTx(tx).admin());
    }

    @Override
    public List<Answer> execute() {
        return stream().collect(toList());
    }

    @Override
    public <T> Stream<T> results(GraqlConverter<?, T> converter) {
        return stream().map(converter::convert);
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public Stream<Answer> stream() {
        return match().stream().map(result -> result.project(vars())).distinct();
    }

    @Override
    public String toString() {
        return match().toString() + " get " + vars().stream().map(Object::toString).collect(joining(", ")) + ";";
    }
}
