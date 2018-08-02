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

import java.util.stream.Stream;

/**
 * Abstract class for queries that have a single result such as {@link DefineQueryImpl} and {@link AggregateQueryImpl}.
 *
 * @param <T> The type of result to return
 *
 * @author Felix Chapman
 */
public abstract class AbstractExecutableQuery<T> extends AbstractQuery<T, T> {

    @Override
    protected final Stream<T> stream() {
        return Stream.of(execute());
    }
}
