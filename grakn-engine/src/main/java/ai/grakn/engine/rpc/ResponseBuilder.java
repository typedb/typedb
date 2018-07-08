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
import ai.grakn.rpc.proto.ConceptProto;
import ai.grakn.rpc.proto.IteratorProto;
import ai.grakn.rpc.proto.SessionProto;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import javax.annotation.Nullable;

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
                    .setOpen(SessionProto.Transaction.Open.Res.getDefaultInstance())
                    .build();
        }

        static SessionProto.Transaction.Res commit() {
            return SessionProto.Transaction.Res.newBuilder()
                    .setCommit(SessionProto.Transaction.Commit.Res.getDefaultInstance())
                    .build();
        }

        static SessionProto.Transaction.Res query(@Nullable IteratorProto.IteratorId iteratorId) {
            SessionProto.Transaction.Query.Res.Builder res = SessionProto.Transaction.Query.Res.newBuilder();
            if (iteratorId == null) {
                res.setNull(ConceptProto.Null.getDefaultInstance());
            } else {
                res.setIteratorId(iteratorId);
            }
            return SessionProto.Transaction.Res.newBuilder().setQuery(res).build();
        }

        static SessionProto.Transaction.Res getSchemaConcept(@Nullable Concept concept) {
            SessionProto.Transaction.GetSchemaConcept.Res.Builder res = SessionProto.Transaction.GetSchemaConcept.Res.newBuilder();
            if (concept == null) {
                res.setNull(ConceptProto.Null.getDefaultInstance());
            } else {
                res.setConcept(ConceptBuilder.concept(concept));
            }
            return SessionProto.Transaction.Res.newBuilder().setGetSchemaConcept(res).build();
        }

        static SessionProto.Transaction.Res getConcept(@Nullable Concept concept) {
            SessionProto.Transaction.GetConcept.Res.Builder res = SessionProto.Transaction.GetConcept.Res.newBuilder();
            if (concept == null) {
                res.setNull(ConceptProto.Null.getDefaultInstance());
            } else {
                res.setConcept(ConceptBuilder.concept(concept));
            }
            return SessionProto.Transaction.Res.newBuilder().setGetConcept(res).build();
        }

        static SessionProto.Transaction.Res getAttributes(IteratorProto.IteratorId iteratorId) {
            SessionProto.Transaction.GetAttributes.Res.Builder res = SessionProto.Transaction.GetAttributes.Res.newBuilder()
                    .setIteratorId(iteratorId);
            return SessionProto.Transaction.Res.newBuilder().setGetAttributes(res).build();
        }

        static SessionProto.Transaction.Res putEntityType(Concept concept) {
            SessionProto.Transaction.PutEntityType.Res.Builder res = SessionProto.Transaction.PutEntityType.Res.newBuilder()
                    .setConcept(ConceptBuilder.concept(concept));
            return SessionProto.Transaction.Res.newBuilder().setPutEntityType(res).build();
        }

        static SessionProto.Transaction.Res putAttributeType(Concept concept) {
            SessionProto.Transaction.PutAttributeType.Res.Builder res = SessionProto.Transaction.PutAttributeType.Res.newBuilder()
                    .setConcept(ConceptBuilder.concept(concept));
            return SessionProto.Transaction.Res.newBuilder().setPutAttributeType(res).build();
        }

        static SessionProto.Transaction.Res putRelationshipType(Concept concept) {
            SessionProto.Transaction.PutRelationshipType.Res.Builder res = SessionProto.Transaction.PutRelationshipType.Res.newBuilder()
                    .setConcept(ConceptBuilder.concept(concept));
            return SessionProto.Transaction.Res.newBuilder().setPutRelationshipType(res).build();
        }

        static SessionProto.Transaction.Res putRole(Concept concept) {
            SessionProto.Transaction.PutRole.Res.Builder res = SessionProto.Transaction.PutRole.Res.newBuilder()
                    .setConcept(ConceptBuilder.concept(concept));
            return SessionProto.Transaction.Res.newBuilder().setPutRole(res).build();
        }

        static SessionProto.Transaction.Res putRule(Concept concept) {
            SessionProto.Transaction.PutRule.Res.Builder res = SessionProto.Transaction.PutRule.Res.newBuilder()
                    .setConcept(ConceptBuilder.concept(concept));
            return SessionProto.Transaction.Res.newBuilder().setPutRule(res).build();
        }

        static SessionProto.Transaction.Res answer(Object object) {
            return SessionProto.Transaction.Res.newBuilder().setAnswer(ConceptBuilder.answer(object)).build();
        }

        static SessionProto.Transaction.Res done() {
            return SessionProto.Transaction.Res.newBuilder().setDone(SessionProto.Transaction.Done.getDefaultInstance()).build();
        }

        static SessionProto.Transaction.Res concept(Concept concept) {
            return SessionProto.Transaction.Res.newBuilder().setConcept(ConceptBuilder.concept(concept)).build();
        }

        static SessionProto.Transaction.Res rolePlayer(Role role, Thing player) {
            ConceptProto.Relation.RolePlayer rolePlayer = ConceptProto.Relation.RolePlayer.newBuilder()
                    .setRole(ConceptBuilder.concept(role))
                    .setPlayer(ConceptBuilder.concept(player))
                    .build();
            return SessionProto.Transaction.Res.newBuilder().setRolePlayer(rolePlayer).build();
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
