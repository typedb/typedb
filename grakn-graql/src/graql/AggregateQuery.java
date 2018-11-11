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

package grakn.core.graql;

import grakn.core.GraknTx;
import grakn.core.graql.answer.Answer;

import javax.annotation.Nullable;

/**
 * An aggregate query produced from a {@link Match}.
 *
 * @param <T> the type of the result of the aggregate query
 *
 */
public interface AggregateQuery<T extends Answer> extends Query<T> {

    @Override
    AggregateQuery<T> withTx(GraknTx tx);

    /**
     * Get the {@link Match} that this {@link AggregateQuery} will operate on.
     */
    @Nullable
    Match match();

    /**
     * Get the {@link Aggregate} that will be executed against the results of the {@link #match()}.
     */
    Aggregate<T> aggregate();
}
