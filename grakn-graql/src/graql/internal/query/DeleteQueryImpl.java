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

package ai.grakn.graql.internal.query;

import ai.grakn.GraknTx;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.DeleteQueryAdmin;
import ai.grakn.graql.answer.ConceptSet;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

/**
 * A {@link DeleteQuery} that will execute deletions for every result of a {@link Match}
 *
 * @author Grakn Warriors
 */
@AutoValue
abstract class DeleteQueryImpl implements DeleteQueryAdmin {

    /**
     * @param vars a collection of variables to delete
     * @param match a pattern to match and delete for each result
     */
    static DeleteQueryImpl of(Collection<? extends Var> vars, Match match) {
        return new AutoValue_DeleteQueryImpl(match, ImmutableSet.copyOf(vars));
    }

    @Override
    public Stream<ConceptSet> stream() {
        return executor().run(this);
    }

    @Override
    public final boolean isReadOnly() {
        return false;
    }

    @Override
    public final GraknTx tx() {
        return match().admin().tx();
    }

    @Override
    public DeleteQuery withTx(GraknTx tx) {
        return Queries.delete(match().withTx(tx).admin(), vars());
    }

    @Override
    public DeleteQueryAdmin admin() {
        return this;
    }

    @Override
    public String toString() {
        StringBuilder query = new StringBuilder();
        query.append(match()).append(" ").append("delete");
        if(!vars().isEmpty()) query.append(" ").append(vars().stream().map(Var::toString).collect(joining(", ")).trim());
        query.append(";");

        return query.toString();
    }

    @Override
    public final Boolean inferring() {
        return match().admin().inferring();
    }
}
