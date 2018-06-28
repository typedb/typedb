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

package ai.grakn.engine.rpc;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.GraknException;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.exception.PropertyNotUniqueException;
import ai.grakn.exception.TemporaryWriteException;
import ai.grakn.graql.Pattern;
import ai.grakn.rpc.proto.ConceptProto;
import ai.grakn.rpc.proto.KeyspaceProto;
import ai.grakn.rpc.proto.TransactionProto;
import ai.grakn.rpc.proto.IteratorProto;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.stream.Stream;

/**
 * A utility class to build RPC Responses from a provided set of Grakn concepts.
 */
public class ResponseBuilder {

    /**
     * An RPC Response Builder class for Transaction Service
     */
    public static class Transaction {

        static TransactionProto.TxResponse done() {
            return TransactionProto.TxResponse.newBuilder().setDone(TransactionProto.Done.getDefaultInstance()).build();
        }

        static TransactionProto.TxResponse noResult() {
            return TransactionProto.TxResponse.newBuilder().setNoResult(true).build();
        }

        static TransactionProto.TxResponse iteratorId(Stream<TransactionProto.TxResponse> responses, TransactionService.Iterators iterators) {
            IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
            ConceptProto.ConceptResponse conceptResponse = ConceptProto.ConceptResponse.newBuilder().setIteratorId(iteratorId).build();
            return TransactionProto.TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
        }

        static TransactionProto.TxResponse concept(Concept concept) {
            return TransactionProto.TxResponse.newBuilder().setConcept(ConceptBuilder.concept(concept)).build();
        }

        static TransactionProto.TxResponse rolePlayer(Role role, Thing player) {
            ConceptProto.RolePlayer rolePlayer = ConceptProto.RolePlayer.newBuilder()
                    .setRole(ConceptBuilder.concept(role))
                    .setPlayer(ConceptBuilder.concept(player))
                    .build();
            return TransactionProto.TxResponse.newBuilder().setRolePlayer(rolePlayer).build();
        }

        static TransactionProto.TxResponse answer(Object object) {
            return TransactionProto.TxResponse.newBuilder().setAnswer(ConceptBuilder.answer(object)).build();
        }

        static TransactionProto.TxResponse conceptResponseWithNoResult() {
            ConceptProto.ConceptResponse conceptResponse = ConceptProto.ConceptResponse.newBuilder().setNoResult(true).build();
            return TransactionProto.TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
        }

        static TransactionProto.TxResponse conceptResopnseWithConcept(Concept concept) {
            ConceptProto.ConceptResponse conceptResponse = ConceptProto.ConceptResponse.newBuilder().setConcept(ConceptBuilder.concept(concept)).build();
            return TransactionProto.TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
        }

        static TransactionProto.TxResponse conceptResponseWithDataType(AttributeType.DataType<?> dataType) {
            ConceptProto.ConceptResponse.Builder conceptResponse = ConceptProto.ConceptResponse.newBuilder();
            conceptResponse.setDataType(ConceptBuilder.dataType(dataType));
            return TransactionProto.TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
        }

        static TransactionProto.TxResponse conceptResponseWithAttributeValue(Object value) {
            ConceptProto.ConceptResponse conceptResponse = ConceptProto.ConceptResponse.newBuilder().setAttributeValue(ConceptBuilder.attributeValue(value)).build();
            return TransactionProto.TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
        }

        static TransactionProto.TxResponse conceptResponseWithPattern(Pattern pattern) {
            ConceptProto.ConceptResponse.Builder conceptResponse = ConceptProto.ConceptResponse.newBuilder();
            if (pattern != null) {
                conceptResponse.setPattern(pattern.toString());
            } else {
                conceptResponse.setNoResult(true);
            }
            return TransactionProto.TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
        }

        static TransactionProto.TxResponse conceptResponseWithRegex(String regex) {
            ConceptProto.ConceptResponse conceptResponse = ConceptProto.ConceptResponse.newBuilder().setRegex(regex).build();
            return TransactionProto.TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
        }
    }

    /**
     * An RPC Response Builder class for Keyspace Service
     */
    public static class Keyspace {

        static KeyspaceProto.Delete.Res delete() {
            return KeyspaceProto.Delete.Res.newBuilder().build();
        }
    }

    public static StatusRuntimeException exception(RuntimeException e) {

        if (e instanceof GraknException) {
            GraknException ge = (GraknException) e;
            String message = ge.getName() + "-" + ge.getMessage();
            if (e instanceof TemporaryWriteException) {
                return exception(Status.RESOURCE_EXHAUSTED, message);
            } else if (e instanceof GraknBackendException) {
                return exception(Status.INTERNAL, message);
            } else if (e instanceof PropertyNotUniqueException) {
                return exception(Status.ALREADY_EXISTS, message);
            } else if (e instanceof GraknTxOperationException | e instanceof GraqlQueryException |
                    e instanceof GraqlSyntaxException | e instanceof InvalidKBException) {
                return exception(Status.INVALID_ARGUMENT, message);
            }
        } else if (e instanceof StatusRuntimeException) {
            return (StatusRuntimeException) e;
        }

        return exception(Status.UNKNOWN, e.getMessage());
    }

    private static StatusRuntimeException exception(Status status, String message) {
        return exception(status.withDescription(message + ". Please check server logs for the stack trace."));
    }

    public static StatusRuntimeException exception(Status status) {
        return new StatusRuntimeException(status);
    }
}
