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

package ai.grakn;

import ai.grakn.graql.ComputeQuery;

import java.util.stream.Stream;

/**
 * Class representing a job executing a {@link ComputeQuery} against a knowledge base.
 * @param <T> return type of ComputeQuery
 */
public interface ComputeExecutor<T> {

    /**
     * Get the result of the compute query job
     *
     * @throws RuntimeException if the job is killed
     */
    Stream<T> stream();

    /**
     * Stop the job executing. Any calls to {@link #stream()} will throw.
     */
    void kill();
}
