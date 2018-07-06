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
import ai.grakn.concept.SchemaConcept;
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
            ConceptProto.Relations.RolePlayer rolePlayer = ConceptProto.Relations.RolePlayer.newBuilder()
                    .setRole(ConceptBuilder.concept(role))
                    .setPlayer(ConceptBuilder.concept(player))
                    .build();
            return SessionProto.Transaction.Res.newBuilder().setRolePlayer(rolePlayer).build();
        }

        /**
         * An RPC Response Builder class for Concept Methods
         */
        public static class ConceptMethod {

            private static SessionProto.Transaction.Res conceptMethodResponse(ConceptProto.Method.Res response) {
                return SessionProto.Transaction.Res.newBuilder()
                        .setConceptMethod(SessionProto.ConceptMethod.Res.newBuilder()
                                .setResponse(response)).build();
            }

            // SchemaConcept methods

            static SessionProto.Transaction.Res isImplicit(boolean implicit) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setIsImplicit(ConceptProto.IsImplicit.Res.newBuilder()
                                .setImplicit(implicit)).build();

                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res getLabel(String label) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetLabel(ConceptProto.GetLabel.Res.newBuilder()
                                .setLabel(label)).build();

                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res getSubConcepts(Stream<? extends SchemaConcept> concepts,
                                                               SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetSubConcepts(ConceptProto.GetSubConcepts.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();

                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res getSuperConcepts(Stream<? extends SchemaConcept> concepts,
                                                                 SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetSuperConcepts(ConceptProto.GetSuperConcepts.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();

                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res getDirectSuperConcept(Concept concept) {
                ConceptProto.GetDirectSuperConcept.Res.Builder responseConcept = ConceptProto.GetDirectSuperConcept.Res.newBuilder();
                if (concept == null) responseConcept.setNull(ConceptProto.Null.getDefaultInstance());
                else responseConcept.setConcept(ConceptBuilder.concept(concept));

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetDirectSuperConcept(responseConcept).build();

                return conceptMethodResponse(response);
            }

            // Rule methods

            static SessionProto.Transaction.Res getWhen(Pattern pattern) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetWhen(ConceptProto.GetWhen.Res.newBuilder()
                                .setPattern(pattern.toString())).build();
                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res getThen(Pattern pattern) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetThen(ConceptProto.GetThen.Res.newBuilder()
                                .setPattern(pattern.toString())).build();
                return conceptMethodResponse(response);
            }

            // Role methods

            static SessionProto.Transaction.Res getRelationshipTypesThatRelateRole(Stream<RelationshipType> concepts,
                                                                                   SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetRelationshipTypesThatRelateRole(ConceptProto.GetRelationshipTypesThatRelateRole.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res getTypesThatPlayRole(Stream<Type> concepts,
                                                                                   SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetTypesThatPlayRole(ConceptProto.GetTypesThatPlayRole.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return conceptMethodResponse(response);
            }

            // Type methods

            static SessionProto.Transaction.Res isAbstract(boolean isAbstract) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setIsAbstract(ConceptProto.IsAbstract.Res.newBuilder()
                                .setAbstract(isAbstract)).build();
                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res getInstances(Stream<? extends Thing> concepts,
                                                             SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetInstances(ConceptProto.GetInstances.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res getAttributeTypes(Stream<AttributeType> concepts,
                                                                  SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetAttributeTypes(ConceptProto.GetAttributeTypes.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res getKeyTypes(Stream<AttributeType> concepts,
                                                            SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetKeyTypes(ConceptProto.GetKeyTypes.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res getRolesPlayedByType(Stream<Role> concepts,
                                                            SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetRolesPlayedByType(ConceptProto.GetRolesPlayedByType.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return conceptMethodResponse(response);
            }

            // EntityType methods

            static SessionProto.Transaction.Res addEntity(Entity entity) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAddEntity(ConceptProto.AddEntity.Res.newBuilder()
                                .setConcept(ConceptBuilder.concept(entity))).build();
                return conceptMethodResponse(response);
            }

            // RelationshipType methods

            static SessionProto.Transaction.Res addRelationship(Relationship relationship) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAddRelationship(ConceptProto.AddRelationship.Res.newBuilder()
                                .setConcept(ConceptBuilder.concept(relationship))).build();
                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res getRelatedRoles(Stream<Role> roles, SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = roles.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetRelatedRoles(ConceptProto.GetRelatedRoles.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return conceptMethodResponse(response);
            }

            // AttributeType methods

            static SessionProto.Transaction.Res getRegex(String regex) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetRegex(ConceptProto.GetRegex.Res.newBuilder()
                                .setRegex((regex != null) ? regex : "")).build();
                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res getDataTypeOfAttributeType(AttributeType.DataType<?> dataType) {
                ConceptProto.GetDataTypeOfAttributeType.Res.Builder methodResponse =
                        ConceptProto.GetDataTypeOfAttributeType.Res.newBuilder();

                if (dataType == null) methodResponse.setNull(ConceptProto.Null.getDefaultInstance()).build();
                else methodResponse.setDataType(ConceptBuilder.dataType(dataType)).build();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetDataTypeOfAttributeType(methodResponse).build();
                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res getAttribute(Attribute<?> attribute) {
                ConceptProto.GetAttribute.Res.Builder methodResponse = ConceptProto.GetAttribute.Res.newBuilder();
                if (attribute == null) methodResponse.setNull(ConceptProto.Null.getDefaultInstance()).build();
                else methodResponse.setConcept(ConceptBuilder.concept(attribute)).build();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetAttribute(methodResponse).build();
                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res putAttribute(Attribute<?> attribute) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setPutAttribute(ConceptProto.PutAttribute.Res.newBuilder()
                                .setConcept(ConceptBuilder.concept(attribute))).build();
                return conceptMethodResponse(response);
            }

            // Thing methods

            static SessionProto.Transaction.Res isInferred(boolean inferred) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setIsInferred(ConceptProto.IsInferred.Res.newBuilder()
                                .setInferred(inferred)).build();
                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res getDirectType(Concept concept) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetDirectType(ConceptProto.GetDirectType.Res.newBuilder()
                                .setConcept(ConceptBuilder.concept(concept))).build();
                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res getKeys(Stream<Attribute<?>> concepts, SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetKeys(ConceptProto.GetKeys.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res getKeysByTypes(Stream<Attribute<?>> concepts, SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetKeysByTypes(ConceptProto.GetKeysByTypes.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res getAttributesForAnyType(Stream<Attribute<?>> concepts, SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetAttributesForAnyType(ConceptProto.GetAttributesForAnyType.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res getAttributesByTypes(Stream<Attribute<?>> concepts, SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetAttributesByTypes(ConceptProto.GetAttributesByTypes.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res getRelationships(Stream<Relationship> concepts, SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetRelationships(ConceptProto.GetRelationships.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res getRelationshipsByRoles(Stream<Relationship> concepts, SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetRelationshipsByRoles(ConceptProto.GetRelationshipsByRoles.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res getRolesPlayedByThing(Stream<Role> concepts, SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetRolesPlayedByThing(ConceptProto.GetRolesPlayedByThing.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res setAttributeRelationship(Relationship concept) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setSetAttributeRelationship(ConceptProto.SetAttributeRelationship.Res.newBuilder()
                                .setConcept(ConceptBuilder.concept(concept))).build();
                return conceptMethodResponse(response);
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
                        .setRolePlayersMap(ConceptProto.Relations.RolePlayersMap.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res getRolePlayersByRoles(Stream<Thing> concepts, SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRolePlayers(ConceptProto.Relations.RolePlayers.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return conceptMethodResponse(response);
            }

            // Attribute methods

            static SessionProto.Transaction.Res getValue(Object value) {
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setValue(ConceptBuilder.attributeValue(value)).build();
                return conceptMethodResponse(response);
            }

            static SessionProto.Transaction.Res getOwners(Stream<Thing> concepts, SessionService.Iterators iterators) {
                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setOwners(ConceptProto.Attribute.Owners.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();
                return conceptMethodResponse(response);
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
