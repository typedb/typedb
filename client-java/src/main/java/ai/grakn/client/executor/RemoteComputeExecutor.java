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

package ai.grakn.client.executor;

import ai.grakn.ComputeExecutor;
import ai.grakn.graql.answer.Answer;

import java.util.stream.Stream;

/**
 * Represents a compute query executing on a gRPC server.
 */
final class RemoteComputeExecutor<T extends Answer> implements ComputeExecutor<T> {

    private final Stream<T> result;

    private RemoteComputeExecutor(Stream<T> result) {
        this.result = result;
    }

    public static <T extends Answer> RemoteComputeExecutor<T> of(Stream<T> result) {
        return new RemoteComputeExecutor<>(result);
    }

    @Override
    public Stream<T> get() {
        return result;
    }

    @Override
    public void kill() {
        throw new UnsupportedOperationException();
    }
}
