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

package ai.grakn.remote.rpc;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcGrakn;
import ai.grakn.rpc.generated.GrpcGrakn.Commit;
import ai.grakn.rpc.generated.GrpcGrakn.DeleteRequest;
import ai.grakn.rpc.generated.GrpcGrakn.Open;
import ai.grakn.rpc.generated.GrpcGrakn.TxRequest;
import ai.grakn.rpc.generated.GrpcIterator.IteratorId;
import ai.grakn.rpc.generated.GrpcIterator.Next;
import ai.grakn.rpc.generated.GrpcIterator.Stop;
import ai.grakn.rpc.util.ConceptBuilder;
import ai.grakn.util.CommonUtil;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * A utility class to build RPC Requests from a provided set of Grakn concepts.
 *
 * @author Grakn Warriors
 */
public class RequestBuilder {

    public static GrpcGrakn.TxRequest open(Keyspace keyspace, GraknTxType txType) {
        GrpcGrakn.Open openRPC = GrpcGrakn.Open.newBuilder().setKeyspace(keyspace.getValue()).setTxType(txType(txType)).build();

        return TxRequest.newBuilder().setOpen(openRPC).build();
    }

    public static GrpcGrakn.TxRequest commit() {
        return TxRequest.newBuilder().setCommit(Commit.getDefaultInstance()).build();
    }

    public static GrpcGrakn.TxRequest query(Query<?> query) {
        return query(query.toString(), query.inferring());
    }

    public static GrpcGrakn.TxRequest query(String queryString, boolean infer) {
        GrpcGrakn.Query.Builder queryRequest = GrpcGrakn.Query.newBuilder().setQuery(queryString);
        queryRequest.setInfer(infer);
        return TxRequest.newBuilder().setQuery(queryRequest).build();
    }

    public static GrpcGrakn.TxRequest next(IteratorId iteratorId) {
        return TxRequest.newBuilder().setNext(Next.newBuilder().setIteratorId(iteratorId)).build();
    }

    public static GrpcGrakn.TxRequest stop(IteratorId iteratorId) {
        return TxRequest.newBuilder().setStop(Stop.newBuilder().setIteratorId(iteratorId)).build();
    }

    public static GrpcGrakn.TxRequest getConcept(ConceptId id) {
        return TxRequest.newBuilder().setGetConcept(id.getValue()).build();
    }

    public static GrpcGrakn.TxRequest getSchemaConcept(Label label) {
        return TxRequest.newBuilder().setGetSchemaConcept(label.getValue()).build();
    }

    public static GrpcGrakn.TxRequest getAttributesByValue(Object value) {
        return TxRequest.newBuilder().setGetAttributesByValue(attributeValue(value)).build();
    }

    public static GrpcGrakn.TxRequest putEntityType(Label label) {
        return TxRequest.newBuilder().setPutEntityType(label.getValue()).build();
    }

    public static GrpcGrakn.TxRequest putRelationshipType(Label label) {
        return TxRequest.newBuilder().setPutRelationshipType(label.getValue()).build();
    }

    public static GrpcGrakn.TxRequest putAttributeType(Label label, AttributeType.DataType<?> dataType) {
        GrpcGrakn.AttributeType putAttributeType =
                GrpcGrakn.AttributeType.newBuilder().setLabel(label.getValue()).setDataType(dataType(dataType)).build();

        return TxRequest.newBuilder().setPutAttributeType(putAttributeType).build();
    }

    public static GrpcGrakn.TxRequest putRole(Label label) {
        return TxRequest.newBuilder().setPutRole(label.getValue()).build();
    }

    public static GrpcGrakn.TxRequest putRule(Label label, Pattern when, Pattern then) {
        GrpcGrakn.Rule putRule = GrpcGrakn.Rule.newBuilder()
                .setLabel(label.getValue())
                .setWhen(when.toString())
                .setThen(then.toString())
                .build();

        return TxRequest.newBuilder().setPutRule(putRule).build();
    }

    public static GrpcGrakn.TxType txType(GraknTxType txType) {
        switch (txType) {
            case READ:
                return GrpcGrakn.TxType.Read;
            case WRITE:
                return GrpcGrakn.TxType.Write;
            case BATCH:
                return GrpcGrakn.TxType.Batch;
            default:
                throw CommonUtil.unreachableStatement("Unrecognised " + txType);
        }
    }

    public static GrpcGrakn.DeleteRequest delete(Open open) {
        return DeleteRequest.newBuilder().setOpen(open).build();
    }


    public static GrpcConcept.AttributeValue attributeValue(Object value) {
        GrpcConcept.AttributeValue.Builder builder = GrpcConcept.AttributeValue.newBuilder();
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

    public static GrpcConcept.Concepts concepts(Stream<? extends Concept> concepts) {
        GrpcConcept.Concepts.Builder grpcConcepts = GrpcConcept.Concepts.newBuilder();
        grpcConcepts.addAllConcepts(concepts.map(ConceptBuilder::concept).collect(toList()));
        return grpcConcepts.build();
    }

    private static GrpcConcept.DataType dataType(AttributeType.DataType<?> dataType) {
        if (dataType.equals(AttributeType.DataType.STRING)) {
            return GrpcConcept.DataType.String;
        } else if (dataType.equals(AttributeType.DataType.BOOLEAN)) {
            return GrpcConcept.DataType.Boolean;
        } else if (dataType.equals(AttributeType.DataType.INTEGER)) {
            return GrpcConcept.DataType.Integer;
        } else if (dataType.equals(AttributeType.DataType.LONG)) {
            return GrpcConcept.DataType.Long;
        } else if (dataType.equals(AttributeType.DataType.FLOAT)) {
            return GrpcConcept.DataType.Float;
        } else if (dataType.equals(AttributeType.DataType.DOUBLE)) {
            return GrpcConcept.DataType.Double;
        } else if (dataType.equals(AttributeType.DataType.DATE)) {
            return GrpcConcept.DataType.Date;
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised " + dataType);
        }
    }


}
