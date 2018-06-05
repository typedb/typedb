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

package ai.grakn.rpc.util;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.rpc.ConceptMethod;
import ai.grakn.rpc.generated.GrpcGrakn;
import ai.grakn.rpc.generated.GrpcGrakn.Commit;
import ai.grakn.rpc.generated.GrpcGrakn.DeleteRequest;
import ai.grakn.rpc.generated.GrpcGrakn.ExecQuery;
import ai.grakn.rpc.generated.GrpcGrakn.Infer;
import ai.grakn.rpc.generated.GrpcGrakn.Open;
import ai.grakn.rpc.generated.GrpcGrakn.PutAttributeType;
import ai.grakn.rpc.generated.GrpcGrakn.PutRule;
import ai.grakn.rpc.generated.GrpcGrakn.RunConceptMethod;
import ai.grakn.rpc.generated.GrpcGrakn.TxRequest;
import ai.grakn.rpc.generated.GrpcIterator.IteratorId;
import ai.grakn.rpc.generated.GrpcIterator.Next;
import ai.grakn.rpc.generated.GrpcIterator.Stop;
import ai.grakn.util.CommonUtil;

import javax.annotation.Nullable;

/**
 * A utility class to build RPC Requests from a provided set of Grakn concepts.
 *
 * @author Grakn Warriors
 */
public class RequestBuilder {

    public static GrpcGrakn.TxRequest open(Keyspace keyspace, GraknTxType txType) {
        GrpcGrakn.Keyspace keyspaceRPC = GrpcGrakn.Keyspace.newBuilder().setValue(keyspace.getValue()).build();
        GrpcGrakn.Open openRPC = GrpcGrakn.Open.newBuilder().setKeyspace(keyspaceRPC).setTxType(txType(txType)).build();

        return TxRequest.newBuilder().setOpen(openRPC).build();
    }

    public static GrpcGrakn.TxRequest commit() {
        return TxRequest.newBuilder().setCommit(Commit.getDefaultInstance()).build();
    }

    public static GrpcGrakn.TxRequest execQuery(Query<?> query) {
        return execQuery(query.toString(), query.inferring());
    }

    public static GrpcGrakn.TxRequest execQuery(String queryString, @Nullable Boolean infer) {
        GrpcGrakn.Query query = GrpcGrakn.Query.newBuilder().setValue(queryString).build();
        ExecQuery.Builder execQueryRequest = ExecQuery.newBuilder().setQuery(query);
        if (infer != null) {
            execQueryRequest.setInfer(Infer.newBuilder().setValue(infer));
        }
        return TxRequest.newBuilder().setExecQuery(execQueryRequest).build();
    }

    public static GrpcGrakn.TxRequest next(IteratorId iteratorId) {
        return TxRequest.newBuilder().setNext(Next.newBuilder().setIteratorId(iteratorId).build()).build();
    }

    public static GrpcGrakn.TxRequest stop(IteratorId iteratorId) {
        return TxRequest.newBuilder().setStop(Stop.newBuilder().setIteratorId(iteratorId).build()).build();
    }

    public static GrpcGrakn.TxRequest runConceptMethod(ConceptId id, ConceptMethod<?> conceptMethod) {
        RunConceptMethod runConceptMethod = RunConceptMethod.newBuilder()
                .setId(ConceptBuilder.conceptId(id))
                .setConceptMethod(conceptMethod.requestBuilder())
                .build();
        return TxRequest.newBuilder().setRunConceptMethod(runConceptMethod).build();
    }

    public static GrpcGrakn.TxRequest getConcept(ConceptId id) {
        return TxRequest.newBuilder().setGetConcept(ConceptBuilder.conceptId(id)).build();
    }

    public static GrpcGrakn.TxRequest getSchemaConcept(Label label) {
        return TxRequest.newBuilder().setGetSchemaConcept(ConceptBuilder.label(label)).build();
    }

    public static GrpcGrakn.TxRequest getAttributesByValue(Object value) {
        return TxRequest.newBuilder().setGetAttributesByValue(ConceptBuilder.attributeValue(value)).build();
    }

    public static GrpcGrakn.TxRequest putEntityType(Label label) {
        return TxRequest.newBuilder().setPutEntityType(ConceptBuilder.label(label)).build();
    }

    public static GrpcGrakn.TxRequest putRelationshipType(Label label) {
        return TxRequest.newBuilder().setPutRelationshipType(ConceptBuilder.label(label)).build();
    }

    public static GrpcGrakn.TxRequest putAttributeType(Label label, AttributeType.DataType<?> dataType) {
        PutAttributeType putAttributeType =
                PutAttributeType.newBuilder().setLabel(ConceptBuilder.label(label)).setDataType(ConceptBuilder.dataType(dataType)).build();

        return TxRequest.newBuilder().setPutAttributeType(putAttributeType).build();
    }

    public static GrpcGrakn.TxRequest putRole(Label label) {
        return TxRequest.newBuilder().setPutRole(ConceptBuilder.label(label)).build();
    }

    public static GrpcGrakn.TxRequest putRule(Label label, Pattern when, Pattern then) {
        PutRule putRule =
                PutRule.newBuilder().setLabel(ConceptBuilder.label(label)).setWhen(ConceptBuilder.pattern(when)).setThen(ConceptBuilder.pattern(then)).build();

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
}
