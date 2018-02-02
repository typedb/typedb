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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.query;

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
    // TODO maybe this is bad too??
    protected abstract Stream<S> stream();

    @Override
    // TODO: Make this method final once `AbstractComputeQuery` doesn't override it
    public Stream<String> resultsString(Printer printer) {
        return results(printer);
    }

    @Override
    public final <U> Stream<U> results(GraqlConverter<?, U> converter) {
        return stream().map(converter::convert);
    }
}
