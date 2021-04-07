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

package grakn.core.server.common;

import com.google.protobuf.ByteString;
import grabl.tracing.client.GrablTracingThreadStatic;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Options;
import grakn.core.concept.type.AttributeType.ValueType;
import grakn.protocol.ConceptProto;
import grakn.protocol.OptionsProto;
import grakn.protocol.TransactionProto;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static grakn.core.common.collection.Bytes.bytesToUUID;
import static grakn.core.common.exception.ErrorMessage.Server.BAD_VALUE_TYPE;
import static grakn.protocol.OptionsProto.Options.BatchSizeOptCase.BATCH_SIZE;
import static grakn.protocol.OptionsProto.Options.ExplainOptCase.EXPLAIN;
import static grakn.protocol.OptionsProto.Options.InferOptCase.INFER;
import static grakn.protocol.OptionsProto.Options.ParallelOptCase.PARALLEL;
import static grakn.protocol.OptionsProto.Options.PrefetchOptCase.PREFETCH;
import static grakn.protocol.OptionsProto.Options.SchemaLockAcquireTimeoutOptCase.SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS;
import static grakn.protocol.OptionsProto.Options.SessionIdleTimeoutOptCase.SESSION_IDLE_TIMEOUT_MILLIS;
import static grakn.protocol.OptionsProto.Options.TraceInferenceOptCase.TRACE_INFERENCE;

public class RequestReader {

    public static UUID byteStringAsUUID(ByteString byteString) {
        return bytesToUUID(byteString.toByteArray());
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
        if (request.getBatchSizeOptCase().equals(BATCH_SIZE)) {
            options.responseBatchSize(request.getBatchSize());
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
                throw GraknException.of(BAD_VALUE_TYPE, valueType);
        }
    }

    public static Optional<TracingData> getTracingData(TransactionProto.Transaction.Req request) {
        if (GrablTracingThreadStatic.isTracingEnabled()) {
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
