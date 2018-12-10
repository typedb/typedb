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

import grakn.core.graql.answer.Answer;
import grakn.core.server.Transaction;

import javax.annotation.Nullable;
import java.util.stream.Stream;

/**
 * An aggregate query produced from a {@link Match}.
 *
 * @param <T> the type of the result of the aggregate query
 */
public class AggregateQuery<T extends Answer> implements Query<T> {

    private final Match match;
    private final Aggregate<T> aggregate;

    public AggregateQuery(@Nullable Match match, Aggregate<T> aggregate) {
        this.match = match;
        if (aggregate == null) {
            throw new NullPointerException("Null aggregate");
        }
        this.aggregate = aggregate;
    }

    /**
     * Get the {@link Match} that this {@link AggregateQuery} will operate on.
     */
    @Nullable
    public Match match() {
        return match;
    }

    /**
     * Get the {@link Aggregate} that will be executed against the results of the {@link #match()}.
     */
    public Aggregate<T> aggregate() {
        return aggregate;
    }

    @Override
    public final Stream<T> stream() {
        return executor().run(this);
    }

    @Override
    public boolean isReadOnly() {
        //TODO An aggregate query may modify the graph if using a user-defined aggregate method. See TP # 13731.
        return true;
    }

    @Override
    public final Transaction tx() {
        return match().admin().tx();
    }

    @Override
    public final String toString() {
        return match().toString() + " aggregate " + aggregate().toString() + ";";
    }

    @Override
    public final Boolean inferring() {
        return match().admin().inferring();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof AggregateQuery) {
            AggregateQuery<?> that = (AggregateQuery<?>) o;
            return ((this.match == null) ? (that.match() == null) : this.match.equals(that.match()))
                    && (this.aggregate.equals(that.aggregate()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= (match == null) ? 0 : this.match.hashCode();
        h *= 1000003;
        h ^= this.aggregate.hashCode();
        return h;
    }

}
