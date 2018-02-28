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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.grpc;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.GraknException;
import ai.grakn.exception.GraknServerException;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.exception.PropertyNotUniqueException;
import ai.grakn.exception.TemporaryWriteException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.rpc.generated.GraknOuterClass;
import ai.grakn.rpc.generated.GraknOuterClass.AttributeValue;
import ai.grakn.rpc.generated.GraknOuterClass.Commit;
import ai.grakn.rpc.generated.GraknOuterClass.Done;
import ai.grakn.rpc.generated.GraknOuterClass.ExecQuery;
import ai.grakn.rpc.generated.GraknOuterClass.GetConceptProperty;
import ai.grakn.rpc.generated.GraknOuterClass.Infer;
import ai.grakn.rpc.generated.GraknOuterClass.IteratorId;
import ai.grakn.rpc.generated.GraknOuterClass.Next;
import ai.grakn.rpc.generated.GraknOuterClass.Open;
import ai.grakn.rpc.generated.GraknOuterClass.Stop;
import ai.grakn.rpc.generated.GraknOuterClass.TxRequest;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;
import ai.grakn.rpc.generated.GraknOuterClass.TxType;
import ai.grakn.util.CommonUtil;
import io.grpc.Metadata;
import io.grpc.Metadata.AsciiMarshaller;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.function.Function;

/**
 * @author Felix Chapman
 */
public class GrpcUtil {

    static class UnknownGraknException extends GraknException {

        private static final long serialVersionUID = 4354432748314041017L;

        UnknownGraknException(String error) {
            super(error);
        }

        public static UnknownGraknException create(String message) {
            return new UnknownGraknException(message);
        }
    }

    /**
     * Enumeration of all sub-classes of {@link GraknException} that can be thrown during gRPC calls.
     */
    public enum ErrorType {
        // TODO: it's likely some of these will NEVER be thrown normally, so shouldn't be here
        GRAQL_QUERY_EXCEPTION(GraqlQueryException::create),
        GRAQL_SYNTAX_EXCEPTION(GraqlSyntaxException::create),
        GRAKN_TX_OPERATION_EXCEPTION(GraknTxOperationException::create),
        TEMPORARY_WRITE_EXCEPTION(TemporaryWriteException::create),
        GRAKN_SERVER_EXCEPTION(GraknServerException::create),
        PROPERTY_NOT_UNIQUE_EXCEPTION(PropertyNotUniqueException::create),
        INVALID_KB_EXCEPTION(InvalidKBException::create),
        GRAKN_BACKEND_EXCEPTION(GraknBackendException::create),
        UNKNOWN(UnknownGraknException::create);

        private final Function<String, GraknException> converter;

        ErrorType(Function<String, GraknException> converter) {
            this.converter = converter;
        }

        public final GraknException toException(String message) {
            return converter.apply(message);
        }

        private static final AsciiMarshaller<ErrorType> ERROR_TYPE_ASCII_MARSHALLER = new AsciiMarshaller<ErrorType>() {
            @Override
            public String toAsciiString(ErrorType value) {
                return value.name();
            }

            @Override
            public ErrorType parseAsciiString(String serialized) {
                return ErrorType.valueOf(serialized);
            }
        };

        public static final Metadata.Key<ErrorType> KEY = Metadata.Key.of("ErrorType", ERROR_TYPE_ASCII_MARSHALLER);
    }

    public static TxRequest openRequest(Keyspace keyspace, GraknTxType txType) {
        Open.Builder open = Open.newBuilder().setKeyspace(convert(keyspace)).setTxType(convert(txType));
        return TxRequest.newBuilder().setOpen(open).build();
    }

    public static TxRequest commitRequest() {
        return TxRequest.newBuilder().setCommit(Commit.getDefaultInstance()).build();
    }

    public static TxRequest execQueryRequest(Query<?> query) {
        return execQueryRequest(query.toString());
    }

    public static TxRequest execQueryRequest(String queryString) {
        return execQueryRequest(queryString, null);
    }

    public static TxRequest execQueryRequest(String queryString, @Nullable Boolean infer) {
        GraknOuterClass.Query query = GraknOuterClass.Query.newBuilder().setValue(queryString).build();
        ExecQuery.Builder execQueryRequest = ExecQuery.newBuilder().setQuery(query);
        if (infer != null) {
            execQueryRequest.setInfer(Infer.newBuilder().setValue(infer));
        }
        return TxRequest.newBuilder().setExecQuery(execQueryRequest).build();
    }

    public static TxRequest nextRequest(IteratorId iteratorId) {
        return TxRequest.newBuilder().setNext(Next.newBuilder().setIteratorId(iteratorId).build()).build();
    }

    public static TxRequest stopRequest(IteratorId iteratorId) {
        return TxRequest.newBuilder().setStop(Stop.newBuilder().setIteratorId(iteratorId).build()).build();
    }

    public static TxRequest getConceptPropertyRequest(ConceptId id, ai.grakn.grpc.ConceptProperty<?> property) {
        GetConceptProperty getConceptProperty = GetConceptProperty.newBuilder()
                .setId(convert(id))
                .setConceptProperty(property.toGrpc())
                .build();
        return TxRequest.newBuilder().setGetConceptProperty(getConceptProperty).build();
    }

    public static TxResponse doneResponse() {
        return TxResponse.newBuilder().setDone(Done.getDefaultInstance()).build();
    }

    public static TxResponse iteratorResponse(IteratorId iteratorId) {
        return TxResponse.newBuilder().setIteratorId(iteratorId).build();
    }

    public static Keyspace getKeyspace(Open open) {
        return convert(open.getKeyspace());
    }

    public static GraknTxType getTxType(Open open) {
        return convert(open.getTxType());
    }

    public static ConceptId getConceptId(GetConceptProperty getConceptPropertyRequest) {
        return convert(getConceptPropertyRequest.getId());
    }

    public static GraknOuterClass.Concept convert(Concept concept) {
        return GraknOuterClass.Concept.newBuilder()
                .setId(GraknOuterClass.ConceptId.newBuilder().setValue(concept.getId().getValue()).build())
                .setBaseType(getBaseType(concept))
                .build();
    }

    private static GraknTxType convert(TxType txType) {
        switch (txType) {
            case Read:
                return GraknTxType.READ;
            case Write:
                return GraknTxType.WRITE;
            case Batch:
                return GraknTxType.BATCH;
            default:
            case UNRECOGNIZED:
                throw new IllegalArgumentException("Unrecognised " + txType);
        }
    }

    private static TxType convert(GraknTxType txType) {
        switch (txType) {
            case READ:
                return TxType.Read;
            case WRITE:
                return TxType.Write;
            case BATCH:
                return TxType.Batch;
            default:
                throw CommonUtil.unreachableStatement("Unrecognised " + txType);
        }
    }

    private static Keyspace convert(GraknOuterClass.Keyspace keyspace) {
        return Keyspace.of(keyspace.getValue());
    }

    private static GraknOuterClass.Keyspace convert(Keyspace keyspace) {
        return GraknOuterClass.Keyspace.newBuilder().setValue(keyspace.getValue()).build();
    }

    private static GraknOuterClass.ConceptId convert(ConceptId id) {
        return GraknOuterClass.ConceptId.newBuilder().setValue(id.getValue()).build();
    }

    private static ConceptId convert(GraknOuterClass.ConceptId id) {
        return ConceptId.of(id.getValue());
    }

    static GraknOuterClass.Label convert(Label label) {
        return GraknOuterClass.Label.newBuilder().setValue(label.getValue()).build();
    }

    static Label convert(GraknOuterClass.Label label) {
        return Label.of(label.getValue());
    }

    static Object convert(AttributeValue value) {
        switch (value.getValueCase()) {
            case STRING:
                return value.getString();
            case BOOLEAN:
                return value.getBoolean();
            case INTEGER:
                return value.getInteger();
            case LONG:
                return value.getLong();
            case FLOAT:
                return value.getFloat();
            case DOUBLE:
                return value.getDouble();
            case DATE:
                return value.getDate();
            default:
            case VALUE_NOT_SET:
                throw new IllegalArgumentException("Unrecognised " + value);
        }
    }

    static AttributeValue convertValue(Object value) {
        AttributeValue.Builder builder = AttributeValue.newBuilder();
        if (value instanceof String) {
            builder.setString((String) value);
        } else if (value instanceof Boolean) {
            builder.setBoolean((boolean) value);
        } else if (value instanceof Integer) {
            builder.setInteger((int) value);
        } else if (value instanceof Long) {
            builder.setLong((long) value);
        } else if (value instanceof Float) {
            builder.setFloat((float) value);
        } else if (value instanceof Double) {
            builder.setDouble((double) value);
        } else if (value instanceof LocalDateTime) {
            builder.setDate(((LocalDateTime) value).atZone(ZoneId.of("Z")).toInstant().toEpochMilli());
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised " + value);
        }

        return builder.build();
    }

    static AttributeType.DataType<?> convert(GraknOuterClass.DataType dataType) {
        switch (dataType) {
            case String:
                return AttributeType.DataType.STRING;
            case Boolean:
                return AttributeType.DataType.BOOLEAN;
            case Integer:
                return AttributeType.DataType.INTEGER;
            case Long:
                return AttributeType.DataType.LONG;
            case Float:
                return AttributeType.DataType.FLOAT;
            case Double:
                return AttributeType.DataType.DOUBLE;
            case Date:
                return AttributeType.DataType.DATE;
            default:
            case UNRECOGNIZED:
                throw new IllegalArgumentException("Unrecognised " + dataType);
        }
    }

    public static GraknOuterClass.DataType convert(AttributeType.DataType<?> dataType) {
        if (dataType.equals(AttributeType.DataType.STRING)) {
            return GraknOuterClass.DataType.String;
        } else if (dataType.equals(AttributeType.DataType.BOOLEAN)) {
            return GraknOuterClass.DataType.Boolean;
        } else if (dataType.equals(AttributeType.DataType.INTEGER)) {
            return GraknOuterClass.DataType.Integer;
        } else if (dataType.equals(AttributeType.DataType.LONG)) {
            return GraknOuterClass.DataType.Long;
        } else if (dataType.equals(AttributeType.DataType.FLOAT)) {
            return GraknOuterClass.DataType.Float;
        } else if (dataType.equals(AttributeType.DataType.DOUBLE)) {
            return GraknOuterClass.DataType.Double;
        } else if (dataType.equals(AttributeType.DataType.DATE)) {
            return GraknOuterClass.DataType.Date;
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised " + dataType);
        }
    }

    private static GraknOuterClass.BaseType getBaseType(Concept concept) {
        if (concept.isEntityType()) {
            return GraknOuterClass.BaseType.EntityType;
        } else if (concept.isRelationshipType()) {
            return GraknOuterClass.BaseType.RelationshipType;
        } else if (concept.isAttributeType()) {
            return GraknOuterClass.BaseType.AttributeType;
        } else if (concept.isEntity()) {
            return GraknOuterClass.BaseType.Entity;
        } else if (concept.isRelationship()) {
            return GraknOuterClass.BaseType.Relationship;
        } else if (concept.isAttribute()) {
            return GraknOuterClass.BaseType.Attribute;
        } else if (concept.isRole()) {
            return GraknOuterClass.BaseType.Role;
        } else if (concept.isRule()) {
            return GraknOuterClass.BaseType.Rule;
        } else if (concept.isType()) {
            return GraknOuterClass.BaseType.MetaType;
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised concept " + concept);
        }
    }

    public static GraknOuterClass.Pattern convert(Pattern pattern) {
        return GraknOuterClass.Pattern.newBuilder().setValue(pattern.toString()).build();
    }

    public static Pattern convert(GraknOuterClass.Pattern pattern) {
        return Graql.parser().parsePattern(pattern.getValue());
    }
}
