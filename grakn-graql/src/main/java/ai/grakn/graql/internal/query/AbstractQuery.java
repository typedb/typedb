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

import ai.grakn.QueryExecutor;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Query;

import javax.annotation.CheckReturnValue;
import java.util.stream.Stream;

/**
 * Abstract base class used by all implementations of {@link Query}.
 *
 * @param <T> The type of the result to return
 * @param <S> The type of streaming results to return
 *
 * @author Grakn Warriors
 */
abstract class AbstractQuery<T, S> implements Query<T> {

    @CheckReturnValue
    protected abstract Stream<S> stream();

    protected final QueryExecutor executor() {
        if (tx() == null) throw GraqlQueryException.noTx();
        return tx().admin().queryExecutor();
    }
}
