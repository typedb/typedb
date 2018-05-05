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

package ai.grakn;

import ai.grakn.graql.NewComputeQuery;

/**
 * Class representing a job executing a {@link NewComputeQuery} against a knowledge base.
 *
 * @author Felix Chapman
 *
 * @param <T> The returned result of the compute job
 */
public interface ComputeJob<T> {

    /**
     * Get the result of the compute query job
     *
     * @throws RuntimeException if the job is killed
     */
    T get();

    /**
     * Stop the job executing. Any calls to {@link #get()} will throw.
     */
    void kill();
}
