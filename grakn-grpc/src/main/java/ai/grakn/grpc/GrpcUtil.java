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

/**
 * @author Felix Chapman
 */
public class GrpcUtil {
    public static TxRequest openRequest(String keyspaceString, TxType txType) {
        GraknOuterClass.Keyspace keyspace = GraknOuterClass.Keyspace.newBuilder().setValue(keyspaceString).build();
        Open.Builder open = Open.newBuilder().setKeyspace(keyspace).setTxType(txType);
        return TxRequest.newBuilder().setOpen(open).build();
    }

    public static TxRequest commitRequest() {
        return TxRequest.newBuilder().setCommit(Commit.getDefaultInstance()).build();
    }

    public static TxRequest execQueryRequest(String queryString) {
        return execQueryRequest(queryString, Infer.getDefaultInstance());
    }

    public static TxRequest execQueryRequest(String queryString, boolean infer) {
        Infer inferMessage = Infer.newBuilder().setValue(infer).setIsSet(true).build();
        return execQueryRequest(queryString, inferMessage);
    }

    public static TxRequest execQueryRequest(String queryString, Infer infer) {
        GraknOuterClass.Query query = GraknOuterClass.Query.newBuilder().setValue(queryString).build();
        ExecQuery.Builder execQueryRequest = ExecQuery.newBuilder().setQuery(query);
        execQueryRequest.setInfer(infer);
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

    public static GraknTxType convert(TxType txType) {
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

    public static Keyspace convert(GraknOuterClass.Keyspace keyspace) {
        return Keyspace.of(keyspace.getValue());
    }
}
