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

package ai.grakn.graql.internal.query;

import ai.grakn.GraknTx;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

/**
 * Default implementation of {@link GetQuery}
 *
 * @author Grakn Warriors
 */
@AutoValue
public abstract class GetQueryImpl extends AbstractQuery<List<Answer>, Answer> implements GetQuery {

    public abstract ImmutableSet<Var> vars();
    public abstract Match match();

    public static GetQueryImpl of(Match match, ImmutableSet<Var> vars) {
        return new AutoValue_GetQueryImpl(vars, match);
    }

    @Override
    public GetQuery withTx(GraknTx tx) {
        return Queries.get(match().withTx(tx).admin(), vars());
    }

    @Override
    public final GraknTx tx() {
        return match().admin().tx();
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public final Stream<Answer> stream() {
        return executor().run(this);
    }

    @Override
    public String toString() {
        return match().toString() + " get " + vars().stream().map(Object::toString).collect(joining(", ")) + ";";
    }

    @Override
    public final List<Answer> execute() {
        return stream().collect(Collectors.toList());
    }

    @Override
    public final Boolean inferring() {
        return match().admin().inferring();
    }
}
