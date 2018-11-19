/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package grakn.core.client.executor;

import grakn.core.server.ComputeExecutor;
import grakn.core.graql.answer.Answer;

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
    public Stream<T> stream() {
        return result;
    }

    @Override
    public void kill() {
        throw new UnsupportedOperationException();
    }
}
