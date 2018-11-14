/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package grakn.core.client.rpc;

import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Label;
import grakn.core.graql.Pattern;
import grakn.core.graql.Query;
import grakn.core.protocol.ConceptProto;
import grakn.core.protocol.KeyspaceProto;
import grakn.core.protocol.SessionProto;
import grakn.core.util.CommonUtil;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;

import static java.util.stream.Collectors.toList;

/**
 * A utility class to build RPC Requests from a provided set of Grakn concepts.
 */
public class RequestBuilder {

    /**
     * An RPC Request Builder class for Transaction Service
     */
    public static class Transaction {

        public static SessionProto.Transaction.Req open(grakn.core.server.keyspace.Keyspace keyspace, grakn.core.server.Transaction.Type txType) {
            SessionProto.Transaction.Open.Req openRequest = SessionProto.Transaction.Open.Req.newBuilder()
                    .setKeyspace(keyspace.getValue())
                    .setType(SessionProto.Transaction.Type.valueOf(txType.getId()))
                    .build();

            return SessionProto.Transaction.Req.newBuilder().setOpenReq(openRequest).build();
        }

        public static SessionProto.Transaction.Req commit() {
            return SessionProto.Transaction.Req.newBuilder()
                    .setCommitReq(SessionProto.Transaction.Commit.Req.getDefaultInstance())
                    .build();
        }

        public static SessionProto.Transaction.Req query(Query<?> query) {
            return query(query.toString(), query.inferring());
        }

        public static SessionProto.Transaction.Req query(String queryString) {
            return query(queryString, true);
        }

        public static SessionProto.Transaction.Req query(String queryString, boolean infer) {
            SessionProto.Transaction.Query.Req request = SessionProto.Transaction.Query.Req.newBuilder()
                    .setQuery(queryString)
                    .setInfer(infer ? SessionProto.Transaction.Query.INFER.TRUE : SessionProto.Transaction.Query.INFER.FALSE)
                    .build();
            return SessionProto.Transaction.Req.newBuilder().setQueryReq(request).build();
        }

        public static SessionProto.Transaction.Req getSchemaConcept(Label label) {
            return SessionProto.Transaction.Req.newBuilder()
                    .setGetSchemaConceptReq(SessionProto.Transaction.GetSchemaConcept.Req.newBuilder().setLabel(label.getValue()))
                    .build();
        }

        public static SessionProto.Transaction.Req getConcept(ConceptId id) {
            return SessionProto.Transaction.Req.newBuilder()
                    .setGetConceptReq(SessionProto.Transaction.GetConcept.Req.newBuilder().setId(id.getValue()))
                    .build();
        }


        public static SessionProto.Transaction.Req getAttributes(Object value) {
            return SessionProto.Transaction.Req.newBuilder()
                    .setGetAttributesReq(SessionProto.Transaction.GetAttributes.Req.newBuilder()
                            .setValue(Concept.attributeValue(value))
                    ).build();
        }

        public static SessionProto.Transaction.Req putEntityType(Label label) {
            return SessionProto.Transaction.Req.newBuilder()
                    .setPutEntityTypeReq(SessionProto.Transaction.PutEntityType.Req.newBuilder().setLabel(label.getValue()))
                    .build();
        }

        public static SessionProto.Transaction.Req putAttributeType(Label label, AttributeType.DataType<?> dataType) {
            SessionProto.Transaction.PutAttributeType.Req request = SessionProto.Transaction.PutAttributeType.Req.newBuilder()
                    .setLabel(label.getValue())
                    .setDataType(Concept.dataType(dataType))
                    .build();

            return SessionProto.Transaction.Req.newBuilder().setPutAttributeTypeReq(request).build();
        }

        public static SessionProto.Transaction.Req putRelationshipType(Label label) {
            SessionProto.Transaction.PutRelationType.Req request = SessionProto.Transaction.PutRelationType.Req.newBuilder()
                    .setLabel(label.getValue())
                    .build();
            return SessionProto.Transaction.Req.newBuilder().setPutRelationTypeReq(request).build();
        }

        public static SessionProto.Transaction.Req putRole(Label label) {
            SessionProto.Transaction.PutRole.Req request = SessionProto.Transaction.PutRole.Req.newBuilder()
                    .setLabel(label.getValue())
                    .build();
            return SessionProto.Transaction.Req.newBuilder().setPutRoleReq(request).build();
        }

        public static SessionProto.Transaction.Req putRule(Label label, Pattern when, Pattern then) {
            SessionProto.Transaction.PutRule.Req request = SessionProto.Transaction.PutRule.Req.newBuilder()
                    .setLabel(label.getValue())
                    .setWhen(when.toString())
                    .setThen(then.toString())
                    .build();
            return SessionProto.Transaction.Req.newBuilder().setPutRuleReq(request).build();
        }

        public static SessionProto.Transaction.Req iterate(int iteratorId) {
            return SessionProto.Transaction.Req.newBuilder()
                    .setIterateReq(SessionProto.Transaction.Iter.Req.newBuilder()
                            .setId(iteratorId)).build();
        }
    }

    /**
     * An RPC Request Builder class for Concept messages
     */
    public static class Concept {

        public static ConceptProto.Concept concept(grakn.core.graql.concept.Concept concept) {
            return ConceptProto.Concept.newBuilder()
                    .setId(concept.id().getValue())
                    .setBaseType(getBaseType(concept))
                    .build();
        }

        private static ConceptProto.Concept.BASE_TYPE getBaseType(grakn.core.graql.concept.Concept concept) {
            if (concept.isEntityType()) {
                return ConceptProto.Concept.BASE_TYPE.ENTITY_TYPE;
            } else if (concept.isRelationshipType()) {
                return ConceptProto.Concept.BASE_TYPE.RELATION_TYPE;
            } else if (concept.isAttributeType()) {
                return ConceptProto.Concept.BASE_TYPE.ATTRIBUTE_TYPE;
            } else if (concept.isEntity()) {
                return ConceptProto.Concept.BASE_TYPE.ENTITY;
            } else if (concept.isRelationship()) {
                return ConceptProto.Concept.BASE_TYPE.RELATION;
            } else if (concept.isAttribute()) {
                return ConceptProto.Concept.BASE_TYPE.ATTRIBUTE;
            } else if (concept.isRole()) {
                return ConceptProto.Concept.BASE_TYPE.ROLE;
            } else if (concept.isRule()) {
                return ConceptProto.Concept.BASE_TYPE.RULE;
            } else if (concept.isType()) {
                return ConceptProto.Concept.BASE_TYPE.META_TYPE;
            } else {
                throw CommonUtil.unreachableStatement("Unrecognised concept " + concept);
            }
        }

        public static Collection<ConceptProto.Concept> concepts(Collection<grakn.core.graql.concept.Concept> concepts) {
            return concepts.stream().map(Concept::concept).collect(toList());
        }

        public static ConceptProto.ValueObject attributeValue(Object value) {
            ConceptProto.ValueObject.Builder builder = ConceptProto.ValueObject.newBuilder();
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

        public static AttributeType.DataType<?> dataType(ConceptProto.AttributeType.DATA_TYPE dataType) {
            switch (dataType) {
                case STRING:
                    return AttributeType.DataType.STRING;
                case BOOLEAN:
                    return AttributeType.DataType.BOOLEAN;
                case INTEGER:
                    return AttributeType.DataType.INTEGER;
                case LONG:
                    return AttributeType.DataType.LONG;
                case FLOAT:
                    return AttributeType.DataType.FLOAT;
                case DOUBLE:
                    return AttributeType.DataType.DOUBLE;
                case DATE:
                    return AttributeType.DataType.DATE;
                default:
                case UNRECOGNIZED:
                    throw new IllegalArgumentException("Unrecognised " + dataType);
            }
        }

        static ConceptProto.AttributeType.DATA_TYPE dataType(AttributeType.DataType<?> dataType) {
            if (dataType.equals(AttributeType.DataType.STRING)) {
                return ConceptProto.AttributeType.DATA_TYPE.STRING;
            } else if (dataType.equals(AttributeType.DataType.BOOLEAN)) {
                return ConceptProto.AttributeType.DATA_TYPE.BOOLEAN;
            } else if (dataType.equals(AttributeType.DataType.INTEGER)) {
                return ConceptProto.AttributeType.DATA_TYPE.INTEGER;
            } else if (dataType.equals(AttributeType.DataType.LONG)) {
                return ConceptProto.AttributeType.DATA_TYPE.LONG;
            } else if (dataType.equals(AttributeType.DataType.FLOAT)) {
                return ConceptProto.AttributeType.DATA_TYPE.FLOAT;
            } else if (dataType.equals(AttributeType.DataType.DOUBLE)) {
                return ConceptProto.AttributeType.DATA_TYPE.DOUBLE;
            } else if (dataType.equals(AttributeType.DataType.DATE)) {
                return ConceptProto.AttributeType.DATA_TYPE.DATE;
            } else {
                throw CommonUtil.unreachableStatement("Unrecognised " + dataType);
            }
        }
    }

    /**
     * An RPC Request Builder class for Keyspace Service
     */
    public static class Keyspace {

        public static KeyspaceProto.Keyspace.Delete.Req delete(String name) {
            return KeyspaceProto.Keyspace.Delete.Req.newBuilder().setName(name).build();
        }
    }
}
