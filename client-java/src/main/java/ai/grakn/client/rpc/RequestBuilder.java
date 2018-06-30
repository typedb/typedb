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

package ai.grakn.client.rpc;

import ai.grakn.GraknTxType;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.rpc.proto.IteratorProto.IteratorId;
import ai.grakn.rpc.proto.IteratorProto.Next;
import ai.grakn.rpc.proto.IteratorProto.Stop;
import ai.grakn.rpc.proto.KeyspaceProto;
import ai.grakn.rpc.proto.SessionProto;
import ai.grakn.util.CommonUtil;

/**
 * A utility class to build RPC Requests from a provided set of Grakn concepts.
 */
public class RequestBuilder {

    /**
     * An RPC Request Builder class for Transaction Service
     */
    public static class Transaction {

        public static SessionProto.TxRequest open(ai.grakn.Keyspace keyspace, GraknTxType txType) {
            SessionProto.Open.Req openRequest = SessionProto.Open.Req.newBuilder()
                    .setKeyspace(keyspace.getValue())
                    .setTxType(txType(txType))
                    .build();

            return SessionProto.TxRequest.newBuilder().setOpen(openRequest).build();
        }

        public static SessionProto.TxRequest commit() {
            return SessionProto.TxRequest.newBuilder()
                    .setCommit(SessionProto.Commit.Req.getDefaultInstance())
                    .build();
        }

        public static SessionProto.TxRequest query(Query<?> query) {
            return query(query.toString(), query.inferring());
        }

        public static SessionProto.TxRequest query(String queryString, boolean infer) {
            SessionProto.Query.Req queryRequest = SessionProto.Query.Req.newBuilder()
                    .setQuery(queryString)
                    .setInfer(infer)
                    .build();
            return SessionProto.TxRequest.newBuilder().setQuery(queryRequest).build();
        }

        public static SessionProto.TxRequest getConcept(ConceptId id) {
            return SessionProto.TxRequest.newBuilder()
                    .setGetConcept(SessionProto.GetConcept.Req.newBuilder().setId(id.getValue()))
                    .build();
        }


        public static SessionProto.TxRequest getAttributes(Object value) {
            return SessionProto.TxRequest.newBuilder()
                    .setGetAttributes(SessionProto.GetAttributes.Req.newBuilder()
                            .setValue(ConceptBuilder.attributeValue(value))
                    ).build();
        }

        public static SessionProto.TxRequest getSchemaConcept(Label label) {
            return SessionProto.TxRequest.newBuilder()
                    .setGetSchemaConcept(SessionProto.GetSchemaConcept.Req.newBuilder().setLabel(label.getValue()))
                    .build();
        }

        public static SessionProto.TxRequest putEntityType(Label label) {
            return SessionProto.TxRequest.newBuilder()
                    .setPutEntityType(SessionProto.PutEntityType.Req.newBuilder().setLabel(label.getValue()))
                    .build();
        }

        public static SessionProto.TxRequest putRelationshipType(Label label) {
            return SessionProto.TxRequest.newBuilder().setPutRelationshipType(label.getValue()).build();
        }

        public static SessionProto.TxRequest putAttributeType(Label label, AttributeType.DataType<?> dataType) {
            SessionProto.AttributeType putAttributeType =
                    SessionProto.AttributeType.newBuilder().setLabel(label.getValue()).setDataType(ConceptBuilder.dataType(dataType)).build();

            return SessionProto.TxRequest.newBuilder().setPutAttributeType(putAttributeType).build();
        }

        public static SessionProto.TxRequest putRole(Label label) {
            return SessionProto.TxRequest.newBuilder().setPutRole(label.getValue()).build();
        }

        public static SessionProto.TxRequest putRule(Label label, Pattern when, Pattern then) {
            SessionProto.Rule putRule = SessionProto.Rule.newBuilder()
                    .setLabel(label.getValue())
                    .setWhen(when.toString())
                    .setThen(then.toString())
                    .build();

            return SessionProto.TxRequest.newBuilder().setPutRule(putRule).build();
        }

        public static SessionProto.TxType txType(GraknTxType txType) {
            switch (txType) {
                case READ:
                    return SessionProto.TxType.Read;
                case WRITE:
                    return SessionProto.TxType.Write;
                case BATCH:
                    return SessionProto.TxType.Batch;
                default:
                    throw CommonUtil.unreachableStatement("Unrecognised " + txType);
            }
        }

        public static SessionProto.TxRequest next(IteratorId iteratorId) {
            return SessionProto.TxRequest.newBuilder().setNext(Next.newBuilder().setIteratorId(iteratorId)).build();
        }

        public static SessionProto.TxRequest stop(IteratorId iteratorId) {
            return SessionProto.TxRequest.newBuilder().setStop(Stop.newBuilder().setIteratorId(iteratorId)).build();
        }
    }

    /**
     * An RPC Request Builder class for Keyspace Service
     */
    public static class Keyspace {

        public static KeyspaceProto.Delete.Req delete(String name) {
            return KeyspaceProto.Delete.Req.newBuilder().setName(name).build();
        }
    }
}
