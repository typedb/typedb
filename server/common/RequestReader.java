/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.server.common;

import com.google.protobuf.ByteString;
import com.vaticle.factory.tracing.client.FactoryTracingThreadStatic;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.type.AttributeType.ValueType;
import com.vaticle.typedb.protocol.ConceptProto;
import com.vaticle.typedb.protocol.OptionsProto;
import com.vaticle.typedb.protocol.TransactionProto;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.BAD_VALUE_TYPE;
import static com.vaticle.typedb.protocol.OptionsProto.Options.ExplainOptCase.EXPLAIN;
import static com.vaticle.typedb.protocol.OptionsProto.Options.InferOptCase.INFER;
import static com.vaticle.typedb.protocol.OptionsProto.Options.ParallelOptCase.PARALLEL;
import static com.vaticle.typedb.protocol.OptionsProto.Options.PrefetchOptCase.PREFETCH;
import static com.vaticle.typedb.protocol.OptionsProto.Options.PrefetchSizeOptCase.PREFETCH_SIZE;
import static com.vaticle.typedb.protocol.OptionsProto.Options.SchemaLockAcquireTimeoutOptCase.SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS;
import static com.vaticle.typedb.protocol.OptionsProto.Options.SessionIdleTimeoutOptCase.SESSION_IDLE_TIMEOUT_MILLIS;
import static com.vaticle.typedb.protocol.OptionsProto.Options.TraceInferenceOptCase.TRACE_INFERENCE;

public class RequestReader {

    public static UUID byteStringAsUUID(ByteString byteString) {
        return ByteArray.of(byteString.toByteArray()).decodeUUID();
    }

    public static <T extends Options<?, ?>> T applyDefaultOptions(T options, OptionsProto.Options request) {
        if (request.getInferOptCase().equals(INFER)) {
            options.infer(request.getInfer());
        }
        if (request.getTraceInferenceOptCase().equals(TRACE_INFERENCE)) {
            options.traceInference(request.getTraceInference());
        }
        if (request.getExplainOptCase().equals(EXPLAIN)) {
            options.explain(request.getExplain());
        }
        if (request.getParallelOptCase().equals(PARALLEL)) {
            options.parallel(request.getParallel());
        }
        if (request.getPrefetchSizeOptCase().equals(PREFETCH_SIZE)) {
            options.prefetchSize(request.getPrefetchSize());
        }
        if (request.getSessionIdleTimeoutOptCase().equals(SESSION_IDLE_TIMEOUT_MILLIS)) {
            options.sessionIdleTimeoutMillis(request.getSessionIdleTimeoutMillis());
        }
        if (request.getSchemaLockAcquireTimeoutOptCase().equals(SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS)) {
            options.schemaLockTimeoutMillis(request.getSchemaLockAcquireTimeoutMillis());
        }
        if (request.getReadAnyReplicaOptCase().equals(OptionsProto.Options.ReadAnyReplicaOptCase.READ_ANY_REPLICA)) {
            options.readAnyReplica(request.getReadAnyReplica());
        }
        return options;
    }

    public static void applyQueryOptions(Options.Query options, OptionsProto.Options request) {
        if (request.getPrefetchOptCase().equals(PREFETCH)) {
            options.prefetch(request.getPrefetch());
        }
    }

    public static ValueType valueType(ConceptProto.AttributeType.ValueType valueType) {
        switch (valueType) {
            case OBJECT:
                return ValueType.OBJECT;
            case STRING:
                return ValueType.STRING;
            case BOOLEAN:
                return ValueType.BOOLEAN;
            case LONG:
                return ValueType.LONG;
            case DOUBLE:
                return ValueType.DOUBLE;
            case DATETIME:
                return ValueType.DATETIME;
            case UNRECOGNIZED:
            default:
                throw TypeDBException.of(BAD_VALUE_TYPE, valueType);
        }
    }

    public static Optional<TracingData> getTracingData(TransactionProto.Transaction.Req request) {
        if (FactoryTracingThreadStatic.isTracingEnabled()) {
            Map<String, String> metadata = request.getMetadataMap();
            String rootID = metadata.get("traceRootId");
            String parentID = metadata.get("traceParentId");
            if (rootID != null && parentID != null) {
                return Optional.of(new TracingData(rootID, parentID));
            }
        }
        return Optional.empty();
    }
}
