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

package ai.grakn.rpc.util;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.GraknException;
import ai.grakn.exception.GraknServerException;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.exception.PropertyNotUniqueException;
import ai.grakn.exception.TemporaryWriteException;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.printer.Printer;
import ai.grakn.rpc.RolePlayer;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcGrakn;
import io.grpc.Metadata;

import java.util.Optional;
import java.util.function.Function;

/**
 * A utility class to build RPC Responses from a provided set of Grakn concepts.
 *
 * @author Haikal Pribadi
 */
public class ResponseBuilder {

    public static GrpcGrakn.TxResponse done() {
        return GrpcGrakn.TxResponse.newBuilder().setDone(GrpcGrakn.Done.getDefaultInstance()).build();
    }

    public static GrpcGrakn.TxResponse concept(Concept concept) {
        return GrpcGrakn.TxResponse.newBuilder().setConcept(ConceptBuilder.concept(concept)).build();
    }

    //TODO: rename this to conceptOptional once we do so in the concept.proto
    public static GrpcGrakn.TxResponse optionalConcept(Optional<Concept> concept) {
        return GrpcGrakn.TxResponse.newBuilder().setOptionalConcept(ConceptBuilder.optionalConcept(concept)).build();
    }

    public static GrpcGrakn.TxResponse rolePlayer(RolePlayer rolePlayer) {
        return GrpcGrakn.TxResponse.newBuilder().setRolePlayer(ConceptBuilder.rolePlayer(rolePlayer)).build();
    }

    public static GrpcGrakn.TxResponse answer(Object object) {
        GrpcGrakn.Answer answer;

        if (object instanceof Answer) {
            answer = GrpcGrakn.Answer.newBuilder().setQueryAnswer(queryAnswer((Answer) object)).build();
        } else if (object instanceof ComputeQuery.Answer) {
            answer = GrpcGrakn.Answer.newBuilder().setComputeAnswer(computeAnswer((ComputeQuery.Answer) object)).build();
        } else {
            // If not an QueryAnswer or ComputeAnswer, convert to JSON
            answer = GrpcGrakn.Answer.newBuilder().setOtherResult(Printer.jsonPrinter().toString(buildDefault(object))).build();
        }

        return GrpcGrakn.TxResponse.newBuilder().setAnswer(answer).build();
    }

    private static GrpcGrakn.QueryAnswer queryAnswer(Answer answer) {
        GrpcGrakn.QueryAnswer.Builder queryAnswerRPC = GrpcGrakn.QueryAnswer.newBuilder();
        answer.forEach((var, concept) -> {
            GrpcConcept.Concept conceptRps = ConceptBuilder.concept(concept);
            queryAnswerRPC.putQueryAnswer(var.getValue(), conceptRps);
        });

        return queryAnswerRPC.build();
    }

    private static GrpcGrakn.ComputeAnswer computeAnswer(ComputeQuery.Answer computeAnswer) {
        GrpcGrakn.ComputeAnswer.Builder computeAnswerRPC = GrpcGrakn.ComputeAnswer.newBuilder();

        if (computeAnswer.getNumber().isPresent()) {
            computeAnswerRPC.setNumber(number(computeAnswer.getNumber().get()));
        }

        return computeAnswerRPC.build();
    }

    private static GrpcGrakn.Number number(Number number) {
        return GrpcGrakn.Number.newBuilder().setNumber(number.toString()).build();
    }

//    private static GrpcGrakn.Paths paths(List<List<ConceptId>> paths) {
//        GrpcGrakn.Paths.Builder pathsRPC = GrpcGrakn.Paths.newBuilder();
//
//        for (List<ConceptId> path : paths) {
//            GrpcConcept.ConceptIds.Builder pathRPC = GrpcConcept.ConceptIds.newBuilder();
//
//            pathRPC.addAllConceptId(path.output().map());
//        }
//    }

    public static Object buildDefault(Object object) {
        return object;
    }

    public static GrpcGrakn.DeleteResponse delete() {
        return GrpcGrakn.DeleteResponse.getDefaultInstance();
    }

    public static GrpcConcept.OptionalDataType optionalDataType(Optional<AttributeType.DataType<?>> dataType) {
        GrpcConcept.OptionalDataType.Builder builder = GrpcConcept.OptionalDataType.newBuilder();
        return dataType.map(ConceptBuilder::dataType)
                .map(builder::setPresent)
                .orElseGet(() -> builder.setAbsent(GrpcConcept.Unit.getDefaultInstance()))
                .build();
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

        // Enums are meant to be serializable, but functions can't be serialized
        private transient final Function<String, GraknException> converter;

        ErrorType(Function<String, GraknException> converter) {
            this.converter = converter;
        }

        public final GraknException toException(String message) {
            return converter.apply(message);
        }

        private static final Metadata.AsciiMarshaller<ErrorType> ERROR_TYPE_ASCII_MARSHALLER = new Metadata.AsciiMarshaller<ErrorType>() {
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

    static class UnknownGraknException extends GraknException {

        private static final long serialVersionUID = 4354432748314041017L;

        UnknownGraknException(String error) {
            super(error);
        }

        public static UnknownGraknException create(String message) {
            return new UnknownGraknException(message);
        }
    }
}
