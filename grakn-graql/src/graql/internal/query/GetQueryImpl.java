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

package grakn.core.graql.internal.query;

import grakn.core.Transaction;
import grakn.core.graql.GetQuery;
import grakn.core.graql.Match;
import grakn.core.graql.Var;
import grakn.core.graql.answer.ConceptMap;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

/**
 * Default implementation of {@link GetQuery}
 *
 */
@AutoValue
public abstract class GetQueryImpl implements GetQuery {

    public abstract ImmutableSet<Var> vars();
    public abstract Match match();

    public static GetQueryImpl of(Match match, ImmutableSet<Var> vars) {
        return new AutoValue_GetQueryImpl(vars, match);
    }

    @Override
    public GetQuery withTx(Transaction tx) {
        return Queries.get(match().withTx(tx).admin(), vars());
    }

    @Override
    public final Transaction tx() {
        return match().admin().tx();
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public final Stream<ConceptMap> stream() {
        return executor().run(this);
    }

    @Override
    public String toString() {
        return match().toString() + " get " + vars().stream().map(Object::toString).collect(joining(", ")) + ";";
    }

    @Override
    public final Boolean inferring() {
        return match().admin().inferring();
    }
}
