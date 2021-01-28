/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.server.rpc.common;

import grakn.core.common.parameters.Options;
import grakn.protocol.OptionsProto;

import static grakn.protocol.OptionsProto.Options.BatchSizeOptCase.BATCH_SIZE;
import static grakn.protocol.OptionsProto.Options.ExplainOptCase.EXPLAIN;
import static grakn.protocol.OptionsProto.Options.InferOptCase.INFER;
import static grakn.protocol.OptionsProto.Options.PrefetchOptCase.PREFETCH;
import static grakn.protocol.OptionsProto.Options.SchemaLockAcquireTimeoutOptCase.SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS;
import static grakn.protocol.OptionsProto.Options.SessionIdleTimeoutOptCase.SESSION_IDLE_TIMEOUT_MILLIS;

public class RequestReader {

    public static <T extends Options<?, ?>> T setDefaultOptions(T options, OptionsProto.Options request) {
        if (request.getInferOptCase().equals(INFER)) {
            options.infer(request.getInfer());
        }
        if (request.getExplainOptCase().equals(EXPLAIN)) {
            options.explain(request.getExplain());
        }
        if (request.getBatchSizeOptCase().equals(BATCH_SIZE)) {
            options.responseBatchSize(request.getBatchSize());
        }
        if (request.getSessionIdleTimeoutOptCase().equals(SESSION_IDLE_TIMEOUT_MILLIS)) {
            options.sessionIdleTimeoutMillis(request.getSessionIdleTimeoutMillis());
        }
        if (request.getSchemaLockAcquireTimeoutOptCase().equals(SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS)) {
            options.schemaLockAcquireTimeoutMillis(request.getSchemaLockAcquireTimeoutMillis());
        }
        return options;
    }

    public static Options.Query setQueryOptions(Options.Query options, OptionsProto.Options request) {
        if (request.getPrefetchOptCase().equals(PREFETCH)) {
            options.prefetch(request.getPrefetch());
        }
        return options;
    }
}
