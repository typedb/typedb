/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.server.common;

import com.google.protobuf.ByteString;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.type.AttributeType.ValueType;
import com.vaticle.typedb.protocol.ConceptProto;
import com.vaticle.typedb.protocol.OptionsProto;

import java.util.UUID;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.BAD_VALUE_TYPE;

public class RequestReader {

    public static UUID byteStringAsUUID(ByteString byteString) {
        return ByteArray.of(byteString.toByteArray()).decodeUUID();
    }

    public static <T extends Options<?, ?>> T applyDefaultOptions(T options, OptionsProto.Options request) {
        if (request.hasInfer()) {
            options.infer(request.getInfer());
        }
        if (request.hasTraceInference()) {
            options.traceInference(request.getTraceInference());
        }
        if (request.hasExplain()) {
            options.explain(request.getExplain());
        }
        if (request.hasParallel()) {
            options.parallel(request.getParallel());
        }
        if (request.hasPrefetchSize()) {
            options.prefetchSize(request.getPrefetchSize());
        }
        if (request.hasSessionIdleTimeoutMillis()) {
            options.sessionIdleTimeoutMillis(request.getSessionIdleTimeoutMillis());
        }
        if (request.hasTransactionTimeoutMillis()) {
            options.transactionTimeoutMillis(request.getTransactionTimeoutMillis());
        }
        if (request.hasSchemaLockAcquireTimeoutMillis()) {
            options.schemaLockTimeoutMillis(request.getSchemaLockAcquireTimeoutMillis());
        }
        if (request.hasReadAnyReplica()) {
            options.readAnyReplica(request.getReadAnyReplica());
        }
        return options;
    }

    public static void applyQueryOptions(Options.Query options, OptionsProto.Options request) {
        if (request.hasPrefetch()) {
            options.prefetch(request.getPrefetch());
        }
    }

    public static ValueType valueType(ConceptProto.ValueType valueType) {
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
}
