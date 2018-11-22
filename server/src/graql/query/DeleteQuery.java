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

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import grakn.core.graql.answer.ConceptSet;
import grakn.core.server.Transaction;

import javax.annotation.CheckReturnValue;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

/**
 * A query for deleting concepts from a {@link Match} clause.
 * The delete operation to perform is based on what {@link VarPattern} objects
 * are provided to it. If only variable names are provided, then the delete
 * query will delete the concept bound to each given variable name. If property
 * flags are provided, e.g. {@code var("x").has("name")} then only those
 * properties are deleted.
 */
@AutoValue
public abstract class DeleteQuery implements Query<ConceptSet> {

    /**
     * @param vars  a collection of variables to delete
     * @param match a pattern to match and delete for each result
     */
    static DeleteQuery of(Collection<? extends Var> vars, Match match) {
        return new AutoValue_DeleteQuery(match, ImmutableSet.copyOf(vars));
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
    public final Transaction tx() {
        return match().admin().tx();
    }

    @Override
    public DeleteQuery withTx(Transaction tx) {
        return Queries.delete(match().withTx(tx).admin(), vars());
    }

    @CheckReturnValue
    public DeleteQuery admin() {
        return this;
    }

    @Override
    public String toString() {
        StringBuilder query = new StringBuilder();
        query.append(match()).append(" ").append("delete");
        if (!vars().isEmpty())
            query.append(" ").append(vars().stream().map(Var::toString).collect(joining(", ")).trim());
        query.append(";");

        return query.toString();
    }

    @Override
    public final Boolean inferring() {
        return match().admin().inferring();
    }

    /**
     * @return the {@link Match} this delete query is operating on
     */
    @CheckReturnValue
    public abstract Match match();

    /**
     * Get the {@link Var}s to delete on each result of {@link #match()}.
     */
    @CheckReturnValue
    public abstract Set<Var> vars();
}
