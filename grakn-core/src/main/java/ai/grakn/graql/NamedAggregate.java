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

package ai.grakn.graql;

import ai.grakn.graql.admin.Answer;

import javax.annotation.CheckReturnValue;

/**
 * An aggregate operation with an associated name. Used when combining aggregates using the 'select' aggregate.
 * @param <T> the type of the result of the aggregate operation
 *
 * @author Felix Chapman
 */
public interface NamedAggregate<T> {
    /**
     * Get the aggregate this named aggregate represents.
     * @return the aggregate this named aggregate represents
     */
    @CheckReturnValue
    Aggregate<Answer, T> getAggregate();

    /**
     * Get the name of this aggregate.
     * @return the name of this aggregate
     */
    @CheckReturnValue
    String getName();
}
