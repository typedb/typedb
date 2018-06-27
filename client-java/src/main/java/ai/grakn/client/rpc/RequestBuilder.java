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
import ai.grakn.Keyspace;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.rpc.proto.TransactionProto;
import ai.grakn.rpc.proto.TransactionProto.Commit;
import ai.grakn.rpc.proto.TransactionProto.DeleteRequest;
import ai.grakn.rpc.proto.TransactionProto.Open;
import ai.grakn.rpc.proto.TransactionProto.TxRequest;
import ai.grakn.rpc.proto.IteratorProto.IteratorId;
import ai.grakn.rpc.proto.IteratorProto.Next;
import ai.grakn.rpc.proto.IteratorProto.Stop;
import ai.grakn.util.CommonUtil;

/**
 * A utility class to build RPC Requests from a provided set of Grakn concepts.
 *
 * @author Grakn Warriors
 */
public class RequestBuilder {

    public static TransactionProto.TxRequest open(Keyspace keyspace, GraknTxType txType) {
        TransactionProto.Open openRPC = TransactionProto.Open.newBuilder().setKeyspace(keyspace.getValue()).setTxType(txType(txType)).build();

        return TxRequest.newBuilder().setOpen(openRPC).build();
    }

    public static TransactionProto.TxRequest commit() {
        return TxRequest.newBuilder().setCommit(Commit.getDefaultInstance()).build();
    }

    public static TransactionProto.TxRequest query(Query<?> query) {
        return query(query.toString(), query.inferring());
    }

    public static TransactionProto.TxRequest query(String queryString, boolean infer) {
        TransactionProto.Query.Builder queryRequest = TransactionProto.Query.newBuilder().setQuery(queryString);
        queryRequest.setInfer(infer);
        return TxRequest.newBuilder().setQuery(queryRequest).build();
    }

    public static TransactionProto.TxRequest next(IteratorId iteratorId) {
        return TxRequest.newBuilder().setNext(Next.newBuilder().setIteratorId(iteratorId)).build();
    }

    public static TransactionProto.TxRequest stop(IteratorId iteratorId) {
        return TxRequest.newBuilder().setStop(Stop.newBuilder().setIteratorId(iteratorId)).build();
    }

    public static TransactionProto.TxRequest getConcept(ConceptId id) {
        return TxRequest.newBuilder().setGetConcept(id.getValue()).build();
    }

    public static TransactionProto.TxRequest getSchemaConcept(Label label) {
        return TxRequest.newBuilder().setGetSchemaConcept(label.getValue()).build();
    }

    public static TransactionProto.TxRequest getAttributesByValue(Object value) {
        return TxRequest.newBuilder().setGetAttributesByValue(ConceptBuilder.attributeValue(value)).build();
    }

    public static TransactionProto.TxRequest putEntityType(Label label) {
        return TxRequest.newBuilder().setPutEntityType(label.getValue()).build();
    }

    public static TransactionProto.TxRequest putRelationshipType(Label label) {
        return TxRequest.newBuilder().setPutRelationshipType(label.getValue()).build();
    }

    public static TransactionProto.TxRequest putAttributeType(Label label, AttributeType.DataType<?> dataType) {
        TransactionProto.AttributeType putAttributeType =
                TransactionProto.AttributeType.newBuilder().setLabel(label.getValue()).setDataType(ConceptBuilder.dataType(dataType)).build();

        return TxRequest.newBuilder().setPutAttributeType(putAttributeType).build();
    }

    public static TransactionProto.TxRequest putRole(Label label) {
        return TxRequest.newBuilder().setPutRole(label.getValue()).build();
    }

    public static TransactionProto.TxRequest putRule(Label label, Pattern when, Pattern then) {
        TransactionProto.Rule putRule = TransactionProto.Rule.newBuilder()
                .setLabel(label.getValue())
                .setWhen(when.toString())
                .setThen(then.toString())
                .build();

        return TxRequest.newBuilder().setPutRule(putRule).build();
    }

    public static TransactionProto.TxType txType(GraknTxType txType) {
        switch (txType) {
            case READ:
                return TransactionProto.TxType.Read;
            case WRITE:
                return TransactionProto.TxType.Write;
            case BATCH:
                return TransactionProto.TxType.Batch;
            default:
                throw CommonUtil.unreachableStatement("Unrecognised " + txType);
        }
    }

    public static TransactionProto.DeleteRequest delete(Open open) {
        return DeleteRequest.newBuilder().setOpen(open).build();
    }
}
