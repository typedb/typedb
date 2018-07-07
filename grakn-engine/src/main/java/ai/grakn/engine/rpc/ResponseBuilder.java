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

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Entity;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
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
import java.util.Map;
import java.util.Set;
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
                res.setNull(ConceptProto.Null.getDefaultInstance());
            } else {
                res.setIteratorId(iteratorId);
            }
            return SessionProto.Transaction.Res.newBuilder().setQuery(res).build();
        }

        static SessionProto.Transaction.Res getSchemaConcept(@Nullable Concept concept) {
            SessionProto.GetSchemaConcept.Res.Builder res = SessionProto.GetSchemaConcept.Res.newBuilder();
            if (concept == null) {
                res.setNull(ConceptProto.Null.getDefaultInstance());
            } else {
                res.setConcept(ConceptBuilder.concept(concept));
            }
            return SessionProto.Transaction.Res.newBuilder().setGetSchemaConcept(res).build();
        }

        static SessionProto.Transaction.Res getConcept(@Nullable Concept concept) {
            SessionProto.GetConcept.Res.Builder res = SessionProto.GetConcept.Res.newBuilder();
            if (concept == null) {
                res.setNull(ConceptProto.Null.getDefaultInstance());
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

        static SessionProto.Transaction.Res answer(Object object) {
            return SessionProto.Transaction.Res.newBuilder().setAnswer(ConceptBuilder.answer(object)).build();
        }

        static SessionProto.Transaction.Res done() {
            return SessionProto.Transaction.Res.newBuilder().setDone(SessionProto.Done.getDefaultInstance()).build();
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

        /**
         * An RPC Response Builder class for Concept Methods
         */
        public static class ConceptMethod {

            private static SessionProto.Transaction.Res transactionRes(ConceptProto.Method.Res response) {
                return SessionProto.Transaction.Res.newBuilder()
                        .setConceptMethod(SessionProto.ConceptMethod.Res.newBuilder()
                                .setResponse(response)).build();
            }

            // Rule methods

            static SessionProto.Transaction.Res when(Pattern pattern) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setWhen(ConceptProto.Rule.When.Res.newBuilder()
                                .setPattern(pattern.toString())).build();
                return transactionRes(response);
            }

            static SessionProto.Transaction.Res then(Pattern pattern) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThen(ConceptProto.Rule.Then.Res.newBuilder()
                                .setPattern(pattern.toString())).build();
                return transactionRes(response);
            }

            // Role methods

            static SessionProto.Transaction.Res relations(Stream<RelationshipType> concepts,
                                                          SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRelations(ConceptProto.Role.Relations.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return transactionRes(response);
            }

            static SessionProto.Transaction.Res players(Stream<Type> concepts,
                                                        SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setPlayers(ConceptProto.Role.Players.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return transactionRes(response);
            }

            // Type methods

            static SessionProto.Transaction.Res instances(Stream<? extends Thing> concepts,
                                                          SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setInstances(ConceptProto.Type.Instances.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return transactionRes(response);
            }

            static SessionProto.Transaction.Res isAbstract(boolean isAbstract) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setIsAbstract(ConceptProto.Type.IsAbstract.Res.newBuilder()
                                .setAbstract(isAbstract)).build();
                return transactionRes(response);
            }

            static SessionProto.Transaction.Res keys(Stream<AttributeType> concepts,
                                                     SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setKeys(ConceptProto.Type.Keys.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return transactionRes(response);
            }

            static SessionProto.Transaction.Res attributes(Stream<AttributeType> concepts,
                                                           SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttributes(ConceptProto.Type.Attributes.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return transactionRes(response);
            }

            static SessionProto.Transaction.Res playing(Stream<Role> concepts,
                                                        SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setPlaying(ConceptProto.Type.Playing.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return transactionRes(response);
            }

            // EntityType methods

            static SessionProto.Transaction.Res createEntity(Entity entity) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setCreateEntity(ConceptProto.EntityType.Create.Res.newBuilder()
                                .setConcept(ConceptBuilder.concept(entity))).build();
                return transactionRes(response);
            }

            // RelationshipType methods

            static SessionProto.Transaction.Res createRelation(Relationship relationship) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setCreateRelation(ConceptProto.RelationType.Create.Res.newBuilder()
                                .setConcept(ConceptBuilder.concept(relationship))).build();
                return transactionRes(response);
            }

            static SessionProto.Transaction.Res roles(Stream<Role> roles, SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = roles.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRoles(ConceptProto.RelationType.Roles.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return transactionRes(response);
            }

            // AttributeType methods

            static SessionProto.Transaction.Res getRegex(String regex) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetRegex(ConceptProto.AttributeType.GetRegex.Res.newBuilder()
                                .setRegex((regex != null) ? regex : "")).build();
                return transactionRes(response);
            }

            static SessionProto.Transaction.Res dataType(AttributeType.DataType<?> dataType) {
                ConceptProto.AttributeType.DataType.Res.Builder methodResponse =
                        ConceptProto.AttributeType.DataType.Res.newBuilder();

                if (dataType == null) methodResponse.setNull(ConceptProto.Null.getDefaultInstance()).build();
                else methodResponse.setDataType(ConceptBuilder.DATA_TYPE(dataType)).build();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setDataType(methodResponse).build();
                return transactionRes(response);
            }

            static SessionProto.Transaction.Res attribute(Attribute<?> attribute) {
                ConceptProto.AttributeType.Attribute.Res.Builder methodResponse = ConceptProto.AttributeType.Attribute.Res.newBuilder();
                if (attribute == null) methodResponse.setNull(ConceptProto.Null.getDefaultInstance()).build();
                else methodResponse.setConcept(ConceptBuilder.concept(attribute)).build();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttribute(methodResponse).build();
                return transactionRes(response);
            }

            static SessionProto.Transaction.Res createAttribute(Attribute<?> attribute) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setCreateAttribute(ConceptProto.AttributeType.Create.Res.newBuilder()
                                .setConcept(ConceptBuilder.concept(attribute))).build();
                return transactionRes(response);
            }

            // Thing methods

            static SessionProto.Transaction.Res isInferred(boolean inferred) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingIsInferred(ConceptProto.Thing.IsInferred.Res.newBuilder()
                                .setInferred(inferred)).build();
                return transactionRes(response);
            }

            static SessionProto.Transaction.Res type(Concept concept) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingType(ConceptProto.Thing.Type.Res.newBuilder()
                                .setConcept(ConceptBuilder.concept(concept))).build();
                return transactionRes(response);
            }

            static SessionProto.Transaction.Res getKeysByTypes(Stream<Attribute<?>> concepts, SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingKeys(ConceptProto.Thing.Keys.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return transactionRes(response);
            }

            static SessionProto.Transaction.Res getAttributesByTypes(Stream<Attribute<?>> concepts, SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingAttributes(ConceptProto.Thing.Attributes.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return transactionRes(response);
            }

            static SessionProto.Transaction.Res getRelationshipsByRoles(Stream<Relationship> concepts, SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingRelations(ConceptProto.Thing.Relations.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return transactionRes(response);
            }

            static SessionProto.Transaction.Res getRolesPlayedByThing(Stream<Role> concepts, SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingRoles(ConceptProto.Thing.Roles.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return transactionRes(response);
            }

            static SessionProto.Transaction.Res relhas(Relationship concept) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingRelhas(ConceptProto.Thing.Relhas.Res.newBuilder()
                                .setConcept(ConceptBuilder.concept(concept))).build();
                return transactionRes(response);
            }

            // Relationship methods

            static SessionProto.Transaction.Res rolePlayersMap(Map<Role, Set<Thing>> rolePlayers, SessionService.Iterators iterators) {
                Stream.Builder<SessionProto.Transaction.Res> responses = Stream.builder();
                rolePlayers.forEach(
                        (role, players) -> players.forEach(
                                player -> {
                                    System.out.print(role.toString() + " - " + player);
                                    responses.add(ResponseBuilder.Transaction.rolePlayer(role, player));
                                }
                        )
                );
                IteratorProto.IteratorId iteratorId = iterators.add(responses.build().iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRolePlayersMap(ConceptProto.Relation.RolePlayersMap.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return transactionRes(response);
            }

            static SessionProto.Transaction.Res getRolePlayersByRoles(Stream<Thing> concepts, SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map((Thing concept) -> Transaction.concept(concept));
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRolePlayers(ConceptProto.Relation.RolePlayers.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return transactionRes(response);
            }

            // Attribute methods

            static SessionProto.Transaction.Res value(Object value) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setValue(ConceptProto.Attribute.Value.Res.newBuilder()
                                .setValue(ConceptBuilder.attributeValue(value))).build();
                return transactionRes(response);
            }

            static SessionProto.Transaction.Res owners(Stream<Thing> concepts, SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setOwners(ConceptProto.Attribute.Owners.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return transactionRes(response);
            }
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
