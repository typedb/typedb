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

package ai.grakn.client.executor;

/**
 * Represents a compute query executing on a gRPC server.
 *
 * @author Felix Chapman
 *
 * @param <T> The returned result of the compute job
 */
final class RemoteComputeExecutor<T> implements ai.grakn.ComputeExecutor {

    private final T result;

    private RemoteComputeExecutor(T result) {
        this.result = result;
    }

    public static <T> RemoteComputeExecutor<T> of(T result) {
        return new RemoteComputeExecutor<>(result);
    }

    @Override
    public T get() {
        return result;
    }

    @Override
    public void kill() {
        throw new UnsupportedOperationException();
    }
}
