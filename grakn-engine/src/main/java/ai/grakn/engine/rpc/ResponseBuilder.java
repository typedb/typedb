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
import ai.grakn.rpc.proto.IteratorProto;
import ai.grakn.rpc.proto.KeyspaceProto;
import ai.grakn.rpc.proto.SessionProto;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import javax.annotation.Nullable;
import java.util.stream.Stream;

/**
 * A utility class to build RPC Responses from a provided set of Grakn concepts.
 */
public class ResponseBuilder {

    /**
     * An RPC Response Builder class for Transaction Service
     */
    public static class Transaction {

        static SessionProto.Transaction.Res open() {
            return SessionProto.Transaction.Res.newBuilder()
                    .setOpen(SessionProto.Open.Res.getDefaultInstance())
                    .build();
        }

        static SessionProto.Transaction.Res commit() {
            return SessionProto.Transaction.Res.newBuilder()
                    .setCommit(SessionProto.Commit.Res.getDefaultInstance())
                    .build();
        }

        static SessionProto.Transaction.Res query(@Nullable IteratorProto.IteratorId iteratorId) {
            SessionProto.Query.Res.Builder res = SessionProto.Query.Res.newBuilder();
            if (iteratorId == null) {
                res.setNull(SessionProto.Null.getDefaultInstance());
            } else {
                res.setIteratorId(iteratorId);
            }
            return SessionProto.Transaction.Res.newBuilder().setQuery(res).build();
        }

        static SessionProto.Transaction.Res getSchemaConcept(@Nullable Concept concept) {
            SessionProto.GetSchemaConcept.Res.Builder res = SessionProto.GetSchemaConcept.Res.newBuilder();
            if (concept == null) {
                res.setNull(SessionProto.Null.getDefaultInstance());
            } else {
                res.setConcept(ConceptBuilder.concept(concept));
            }
            return SessionProto.Transaction.Res.newBuilder().setGetSchemaConcept(res).build();
        }

        static SessionProto.Transaction.Res getConcept(@Nullable Concept concept) {
            SessionProto.GetConcept.Res.Builder res = SessionProto.GetConcept.Res.newBuilder();
            if (concept == null) {
                res.setNull(SessionProto.Null.getDefaultInstance());
            } else {
                res.setConcept(ConceptBuilder.concept(concept));
            }
            return SessionProto.Transaction.Res.newBuilder().setGetConcept(res).build();
        }

        static SessionProto.Transaction.Res getAttributes(IteratorProto.IteratorId iteratorId) {
            SessionProto.GetAttributes.Res.Builder res = SessionProto.GetAttributes.Res.newBuilder()
                    .setIteratorId(iteratorId);
            return SessionProto.Transaction.Res.newBuilder().setGetAttributes(res).build();
        }

        static SessionProto.Transaction.Res putEntityType(Concept concept) {
            SessionProto.PutEntityType.Res.Builder res = SessionProto.PutEntityType.Res.newBuilder()
                    .setConcept(ConceptBuilder.concept(concept));
            return SessionProto.Transaction.Res.newBuilder().setPutEntityType(res).build();
        }

        static SessionProto.Transaction.Res putAttributeType(Concept concept) {
            SessionProto.PutAttributeType.Res.Builder res = SessionProto.PutAttributeType.Res.newBuilder()
                    .setConcept(ConceptBuilder.concept(concept));
            return SessionProto.Transaction.Res.newBuilder().setPutAttributeType(res).build();
        }

        static SessionProto.Transaction.Res putRelationshipType(Concept concept) {
            SessionProto.PutRelationshipType.Res.Builder res = SessionProto.PutRelationshipType.Res.newBuilder()
                    .setConcept(ConceptBuilder.concept(concept));
            return SessionProto.Transaction.Res.newBuilder().setPutRelationshipType(res).build();
        }

        static SessionProto.Transaction.Res putRole(Concept concept) {
            SessionProto.PutRole.Res.Builder res = SessionProto.PutRole.Res.newBuilder()
                    .setConcept(ConceptBuilder.concept(concept));
            return SessionProto.Transaction.Res.newBuilder().setPutRole(res).build();
        }

        static SessionProto.Transaction.Res putRule(Concept concept) {
            SessionProto.PutRule.Res.Builder res = SessionProto.PutRule.Res.newBuilder()
                    .setConcept(ConceptBuilder.concept(concept));
            return SessionProto.Transaction.Res.newBuilder().setPutRule(res).build();
        }

        static SessionProto.Transaction.Res done() {
            return SessionProto.Transaction.Res.newBuilder().setDone(SessionProto.Done.getDefaultInstance()).build();
        }

        static SessionProto.Transaction.Res iteratorId(Stream<SessionProto.Transaction.Res> responses, SessionService.Iterators iterators) {
            IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
            ConceptProto.Method.Res conceptResponse = ConceptProto.Method.Res.newBuilder()
                    .setIteratorId(iteratorId).build();
            return SessionProto.Transaction.Res.newBuilder().setConceptResponse(conceptResponse).build();
        }

        static SessionProto.Transaction.Res concept(Concept concept) {
            return SessionProto.Transaction.Res.newBuilder().setConcept(ConceptBuilder.concept(concept)).build();
        }

        static SessionProto.Transaction.Res rolePlayer(Role role, Thing player) {
            ConceptProto.RolePlayer rolePlayer = ConceptProto.RolePlayer.newBuilder()
                    .setRole(ConceptBuilder.concept(role))
                    .setPlayer(ConceptBuilder.concept(player))
                    .build();
            return SessionProto.Transaction.Res.newBuilder().setRolePlayer(rolePlayer).build();
        }

        /**
         * An RPC Response Builder class for Concept Methods
         */
        public static class ConceptMethod {

            static SessionProto.Transaction.Res delete() {
                SessionProto.ConceptMethod.Res response = SessionProto.ConceptMethod.Res.newBuilder()
                        .setResponse(ConceptProto.Method.Res.newBuilder()
                                .setDelete(ConceptProto.Delete.Res.getDefaultInstance())).build();
                return SessionProto.Transaction.Res.newBuilder().setConceptMethod(response).build();
            }

            static SessionProto.Transaction.Res getLabel(String label) {
                SessionProto.ConceptMethod.Res response = SessionProto.ConceptMethod.Res.newBuilder()
                        .setResponse(ConceptProto.Method.Res.newBuilder()
                                .setGetLabel(ConceptProto.GetLabel.Res.newBuilder()
                                        .setLabel(label))).build();
                return SessionProto.Transaction.Res.newBuilder().setConceptMethod(response).build();
            }

        }

        static SessionProto.Transaction.Res answer(Object object) {
            return SessionProto.Transaction.Res.newBuilder().setAnswer(ConceptBuilder.answer(object)).build();
        }

        static SessionProto.Transaction.Res conceptResponseWithNoResult() {
            ConceptProto.Method.Res conceptResponse = ConceptProto.Method.Res.newBuilder()
                    .setNoResult(true).build();
            return SessionProto.Transaction.Res.newBuilder().setConceptResponse(conceptResponse).build();
        }

        static SessionProto.Transaction.Res conceptResopnseWithConcept(Concept concept) {
            ConceptProto.Method.Res conceptResponse = ConceptProto.Method.Res.newBuilder()
                    .setConcept(ConceptBuilder.concept(concept)).build();
            return SessionProto.Transaction.Res.newBuilder().setConceptResponse(conceptResponse).build();
        }

        static SessionProto.Transaction.Res conceptResponseWithDataType(AttributeType.DataType<?> dataType) {
            ConceptProto.Method.Res.Builder conceptResponse = ConceptProto.Method.Res.newBuilder();
            conceptResponse.setDataType(ConceptBuilder.dataType(dataType));
            return SessionProto.Transaction.Res.newBuilder().setConceptResponse(conceptResponse).build();
        }

        static SessionProto.Transaction.Res conceptResponseWithAttributeValue(Object value) {
            ConceptProto.Method.Res conceptResponse = ConceptProto.Method.Res.newBuilder()
                    .setAttributeValue(ConceptBuilder.attributeValue(value)).build();
            return SessionProto.Transaction.Res.newBuilder().setConceptResponse(conceptResponse).build();
        }

        static SessionProto.Transaction.Res conceptResponseWithPattern(Pattern pattern) {
            ConceptProto.Method.Res.Builder conceptResponse = ConceptProto.Method.Res.newBuilder();
            if (pattern != null) {
                conceptResponse.setPattern(pattern.toString());
            } else {
                conceptResponse.setNoResult(true);
            }
            return SessionProto.Transaction.Res.newBuilder().setConceptResponse(conceptResponse).build();
        }

        static SessionProto.Transaction.Res conceptResponseWithRegex(String regex) {
            ConceptProto.Method.Res conceptResponse = ConceptProto.Method.Res.newBuilder()
                    .setRegex(regex).build();
            return SessionProto.Transaction.Res.newBuilder().setConceptResponse(conceptResponse).build();
        }
    }

    /**
     * An RPC Response Builder class for Keyspace Service
     */
    public static class Keyspace {

        static KeyspaceProto.Delete.Res delete() {
            return KeyspaceProto.Delete.Res.getDefaultInstance();
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
