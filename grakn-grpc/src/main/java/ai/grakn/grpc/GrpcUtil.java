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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.grpc;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.rpc.generated.GraknOuterClass;
import ai.grakn.rpc.generated.GraknOuterClass.Commit;
import ai.grakn.rpc.generated.GraknOuterClass.Done;
import ai.grakn.rpc.generated.GraknOuterClass.ExecQuery;
import ai.grakn.rpc.generated.GraknOuterClass.Infer;
import ai.grakn.rpc.generated.GraknOuterClass.Next;
import ai.grakn.rpc.generated.GraknOuterClass.Open;
import ai.grakn.rpc.generated.GraknOuterClass.Stop;
import ai.grakn.rpc.generated.GraknOuterClass.TxRequest;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;
import ai.grakn.rpc.generated.GraknOuterClass.TxType;

import javax.annotation.Nullable;

/**
 * @author Felix Chapman
 */
public class GrpcUtil {

    public static TxRequest openRequest(Keyspace keyspace, TxType txType) {
        Open.Builder open = Open.newBuilder().setKeyspace(convert(keyspace)).setTxType(txType);
        return TxRequest.newBuilder().setOpen(open).build();
    }

    public static TxRequest commitRequest() {
        return TxRequest.newBuilder().setCommit(Commit.getDefaultInstance()).build();
    }

    public static TxRequest execQueryRequest(String queryString) {
        return execQueryRequest(queryString, null);
    }

    public static TxRequest execQueryRequest(String queryString, @Nullable Boolean infer) {
        GraknOuterClass.Query query = GraknOuterClass.Query.newBuilder().setValue(queryString).build();
        ExecQuery.Builder execQueryRequest = ExecQuery.newBuilder().setQuery(query);
        if (infer != null) {
            execQueryRequest.setInfer(Infer.newBuilder().setValue(infer));
        }
        return TxRequest.newBuilder().setExecQuery(execQueryRequest).build();
    }

    public static TxRequest nextRequest() {
        return TxRequest.newBuilder().setNext(Next.getDefaultInstance()).build();
    }

    public static TxRequest stopRequest() {
        return TxRequest.newBuilder().setStop(Stop.getDefaultInstance()).build();
    }

    public static TxResponse doneResponse() {
        return TxResponse.newBuilder().setDone(Done.getDefaultInstance()).build();
    }

    public static Keyspace getKeyspace(Open open) {
        return convert(open.getKeyspace());
    }

    public static GraknTxType getTxType(Open open) {
        return convert(open.getTxType());
    }

    private static GraknTxType convert(TxType txType) {
        switch (txType) {
            case Read:
                return GraknTxType.READ;
            case Write:
                return GraknTxType.WRITE;
            case Batch:
                return GraknTxType.BATCH;
            default:
            case UNRECOGNIZED:
                throw new IllegalArgumentException("Unrecognised " + txType);
        }
    }

    private static Keyspace convert(GraknOuterClass.Keyspace keyspace) {
        return Keyspace.of(keyspace.getValue());
    }

    private static GraknOuterClass.Keyspace convert(Keyspace keyspace) {
        return GraknOuterClass.Keyspace.newBuilder().setValue(keyspace.getValue()).build();
    }
}
