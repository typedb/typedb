/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.kb.graql.executor;

/**
 * Factory for retrieving executors either analytical or transactional workloads
 * Ideally, we would further split the executors into further types:
 * `compute` executor
 * `read` executor
 * `insert` executor
 * `delete` executor
 * `schema` executor?
 * (etc, very flexible what this might represent)
 *
 * This will force us to untangle further complicated dependencies and streamline the execution flow of a query,
 * and reduce the dependencies that need to be injected across the board
 */
public interface ExecutorFactory {
    ComputeExecutor compute();
    QueryExecutor transactional(boolean infer);
}
