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
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
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
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.rpc.generated.GraknOuterClass;
import ai.grakn.rpc.generated.GraknOuterClass.Commit;
import ai.grakn.rpc.generated.GraknOuterClass.Done;
import ai.grakn.rpc.generated.GraknOuterClass.ExecQuery;
import ai.grakn.rpc.generated.GraknOuterClass.Infer;
import ai.grakn.rpc.generated.GraknOuterClass.Next;
import ai.grakn.rpc.generated.GraknOuterClass.Open;
import ai.grakn.rpc.generated.GraknOuterClass.QueryResult;
import ai.grakn.rpc.generated.GraknOuterClass.Stop;
import ai.grakn.rpc.generated.GraknOuterClass.TxRequest;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;
import ai.grakn.rpc.generated.GraknOuterClass.TxType;
import ai.grakn.util.CommonUtil;
import com.google.common.collect.ImmutableMap;
import io.grpc.Metadata;
import io.grpc.Metadata.AsciiMarshaller;
import mjson.Json;

import javax.annotation.Nullable;
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

    public static TxRequest nextRequest() {
        return TxRequest.newBuilder().setNext(Next.getDefaultInstance()).build();
    }

    public static TxRequest stopRequest() {
        return TxRequest.newBuilder().setStop(Stop.getDefaultInstance()).build();
    }

    public static TxResponse doneResponse() {
        return TxResponse.newBuilder().setDone(Done.getDefaultInstance()).build();
    }

    public static Keyspace getKeyspace(Open open) {
        return convert(open.getKeyspace());
    }

    public static GraknTxType getTxType(Open open) {
        return convert(open.getTxType());
    }

    public static Object getQueryResult(QueryResult queryResult) {
        switch (queryResult.getQueryResultCase()) {
            case ANSWER:
                return convert(queryResult.getAnswer());
            case OTHERRESULT:
                return Json.read(queryResult.getOtherResult()).getValue();
            default:
            case QUERYRESULT_NOT_SET:
                throw new IllegalArgumentException("Unexpected " + queryResult);
        }
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

    private static Answer convert(GraknOuterClass.Answer answer) {
        ImmutableMap.Builder<Var, Concept> map = ImmutableMap.builder();

        answer.getAnswerMap().forEach((grpcVar, grpcConcept) -> {
            Concept concept = RemoteConcept.create(ConceptId.of(grpcConcept.getId()));
            map.put(Graql.var(grpcVar), concept);
        });

        return new QueryAnswer(map.build());
    }

}
