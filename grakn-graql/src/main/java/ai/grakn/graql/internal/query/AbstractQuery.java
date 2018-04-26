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

import ai.grakn.QueryExecutor;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.GraqlConverter;
import ai.grakn.graql.Printer;
import ai.grakn.graql.Query;

import javax.annotation.CheckReturnValue;
import java.util.stream.Stream;

/**
 * Abstract base class used by all implementations of {@link Query}.
 *
 * @param <T> The type of the result to return
 * @param <S> The type of streaming results to return
 *
 * @author Felix Chapman
 */
abstract class AbstractQuery<T, S> implements Query<T> {

    @CheckReturnValue
    protected abstract Stream<S> stream();

    @Override
    public final Stream<String> resultsString(Printer<?> printer) {
        return results(printer);
    }

    @Override
    public final <U> Stream<U> results(GraqlConverter<?, U> converter) {
        return stream().map(converter::convert);
    }

    protected final QueryExecutor executor() {
        return tx().orElseThrow(GraqlQueryException::noTx).admin().queryExecutor();
    }
}
