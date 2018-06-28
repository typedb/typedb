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
import ai.grakn.rpc.proto.SessionProto;
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

        static SessionProto.TxResponse done() {
            return SessionProto.TxResponse.newBuilder().setDone(SessionProto.Done.getDefaultInstance()).build();
        }

        static SessionProto.TxResponse noResult() {
            return SessionProto.TxResponse.newBuilder().setNoResult(true).build();
        }

        static SessionProto.TxResponse iteratorId(Stream<SessionProto.TxResponse> responses, SessionService.Iterators iterators) {
            IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
            ConceptProto.ConceptResponse conceptResponse = ConceptProto.ConceptResponse.newBuilder().setIteratorId(iteratorId).build();
            return SessionProto.TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
        }

        static SessionProto.TxResponse concept(Concept concept) {
            return SessionProto.TxResponse.newBuilder().setConcept(ConceptBuilder.concept(concept)).build();
        }

        static SessionProto.TxResponse rolePlayer(Role role, Thing player) {
            ConceptProto.RolePlayer rolePlayer = ConceptProto.RolePlayer.newBuilder()
                    .setRole(ConceptBuilder.concept(role))
                    .setPlayer(ConceptBuilder.concept(player))
                    .build();
            return SessionProto.TxResponse.newBuilder().setRolePlayer(rolePlayer).build();
        }

        static SessionProto.TxResponse answer(Object object) {
            return SessionProto.TxResponse.newBuilder().setAnswer(ConceptBuilder.answer(object)).build();
        }

        static SessionProto.TxResponse conceptResponseWithNoResult() {
            ConceptProto.ConceptResponse conceptResponse = ConceptProto.ConceptResponse.newBuilder().setNoResult(true).build();
            return SessionProto.TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
        }

        static SessionProto.TxResponse conceptResopnseWithConcept(Concept concept) {
            ConceptProto.ConceptResponse conceptResponse = ConceptProto.ConceptResponse.newBuilder().setConcept(ConceptBuilder.concept(concept)).build();
            return SessionProto.TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
        }

        static SessionProto.TxResponse conceptResponseWithDataType(AttributeType.DataType<?> dataType) {
            ConceptProto.ConceptResponse.Builder conceptResponse = ConceptProto.ConceptResponse.newBuilder();
            conceptResponse.setDataType(ConceptBuilder.dataType(dataType));
            return SessionProto.TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
        }

        static SessionProto.TxResponse conceptResponseWithAttributeValue(Object value) {
            ConceptProto.ConceptResponse conceptResponse = ConceptProto.ConceptResponse.newBuilder().setAttributeValue(ConceptBuilder.attributeValue(value)).build();
            return SessionProto.TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
        }

        static SessionProto.TxResponse conceptResponseWithPattern(Pattern pattern) {
            ConceptProto.ConceptResponse.Builder conceptResponse = ConceptProto.ConceptResponse.newBuilder();
            if (pattern != null) {
                conceptResponse.setPattern(pattern.toString());
            } else {
                conceptResponse.setNoResult(true);
            }
            return SessionProto.TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
        }

        static SessionProto.TxResponse conceptResponseWithRegex(String regex) {
            ConceptProto.ConceptResponse conceptResponse = ConceptProto.ConceptResponse.newBuilder().setRegex(regex).build();
            return SessionProto.TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
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
