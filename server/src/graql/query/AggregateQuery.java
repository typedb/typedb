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

import javax.annotation.Nullable;

/**
 * An aggregate query produced from a match clause
 *
 * @param <T> the type of the result of the aggregate query
 */
public class AggregateQuery<T extends Answer> implements Query<T> {

    private final MatchClause match;
    private final Aggregate<T> aggregate;

    public AggregateQuery(@Nullable MatchClause match, Aggregate<T> aggregate) {
        this.match = match;
        if (aggregate == null) {
            throw new NullPointerException("Null aggregate");
        }
        this.aggregate = aggregate;
    }

    @Nullable
    public MatchClause match() {
        return match;
    }

    public Aggregate<T> aggregate() {
        return aggregate;
    }

    @Override
    public final String toString() {
        StringBuilder query = new StringBuilder();

        query.append(match()).append(Char.SPACE)
                .append(Command.AGGREGATE).append(Char.SPACE).append(aggregate)
                .append(Char.SEMICOLON);

        return query.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AggregateQuery<?> that = (AggregateQuery<?>) o;

        return (this.match == null ? that.match() == null : this.match.equals(that.match()) &&
                this.aggregate.equals(that.aggregate()));
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
