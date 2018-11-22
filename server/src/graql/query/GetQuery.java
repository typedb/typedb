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

package grakn.core.graql.query;

import grakn.core.server.Transaction;
import grakn.core.graql.answer.ConceptMap;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

/**
 * A query used for finding data in a knowledge base that matches the given patterns. The {@link GetQuery} is a
 * pattern-matching query. The patterns are described in a declarative fashion, then the {@link GetQuery} will traverse
 * the knowledge base in an efficient fashion to find any matching answers.
 */
@AutoValue
public abstract class GetQuery implements Query<ConceptMap> {

    @javax.annotation.CheckReturnValue
    public abstract ImmutableSet<Var> vars();
    @javax.annotation.CheckReturnValue
    public abstract Match match();

    public static GetQuery of(Match match, ImmutableSet<Var> vars) {
        return new AutoValue_GetQuery(vars, match);
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
