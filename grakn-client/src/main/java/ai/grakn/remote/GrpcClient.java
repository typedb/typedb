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

package ai.grakn.remote;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.exception.GraknException;
import ai.grakn.graql.Query;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.rpc.generated.GraknOuterClass;
import ai.grakn.rpc.generated.GraknOuterClass.Commit;
import ai.grakn.rpc.generated.GraknOuterClass.ExecQuery;
import ai.grakn.rpc.generated.GraknOuterClass.Infer;
import ai.grakn.rpc.generated.GraknOuterClass.Open;
import ai.grakn.rpc.generated.GraknOuterClass.TxRequest;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;
import ai.grakn.rpc.generated.GraknOuterClass.TxType;
import ai.grakn.util.CommonUtil;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import javax.annotation.Nullable;

/**
 * Communicates with a Grakn gRPC server, translating requests and responses to and from their gRPC representations.
 *
 * <p>
 *     This class is a light abstraction layer over gRPC - it understands how the sequence of calls should execute and
 *     how to translate gRPC objects into Java objects and back. However, any logic is kept in {@link GraknRemoteTx}.
 * </p>
 *
 * @author Felix Chapman
 */
class GrpcClient implements AutoCloseable {

    private final SynchronousObserver<TxRequest, TxResponse, StatusRuntimeException> observer;

    private GrpcClient(SynchronousObserver<TxRequest, TxResponse, StatusRuntimeException> observer) {
        this.observer = observer;
    }

    public static GrpcClient create(GraknGrpc.GraknStub stub) {
        SynchronousObserver<TxRequest, TxResponse, StatusRuntimeException> observer =
                SynchronousObserver.create(stub::tx);

        return new GrpcClient(observer);
    }

    public void open(Keyspace keyspace, GraknTxType txType) {
        GraknOuterClass.Keyspace grpcKeyspace = GraknOuterClass.Keyspace.newBuilder().setValue(keyspace.getValue()).build();
        TxType grpcTxType = convertTxType(txType);
        Open open = Open.newBuilder().setKeyspace(grpcKeyspace).setTxType(grpcTxType).build();

        observer.send(TxRequest.newBuilder().setOpen(open).build());

        StatusRuntimeException error = observer.receive().throwable();
        if (error != null) {
            throw convertStatusRuntimeException(error);
        }
    }

    public void execQuery(Query<?> query, @Nullable Boolean infer) {
        GraknOuterClass.Query grpcQuery = convertQuery(query);
        ExecQuery execQuery = ExecQuery.newBuilder().setQuery(grpcQuery).setInfer(infer(infer)).build();
        observer.send(TxRequest.newBuilder().setExecQuery(execQuery).build());
    }

    public void commit() {
        observer.send(TxRequest.newBuilder().setCommit(Commit.getDefaultInstance()).build());

        StatusRuntimeException error = observer.receive().throwable();
        if (error != null) {
            throw convertStatusRuntimeException(error);
        }
    }

    @Override
    public void close() {
        observer.close();
    }

    private static Infer infer(@Nullable Boolean infer) {
        Infer.Builder grpcInfer = Infer.newBuilder();
        if (infer != null) {
            grpcInfer.setIsSet(true);
            grpcInfer.setValue(infer);
        }
        return grpcInfer.build();
    }

    private static GraknOuterClass.Query convertQuery(Query<?> query) {
        return GraknOuterClass.Query.newBuilder().setValue(query.toString()).build();
    }

    private static TxType convertTxType(GraknTxType txType) {
        switch (txType) {
            case READ:
                return TxType.Read;
            case WRITE:
                return TxType.Write;
            case BATCH:
                return TxType.Batch;
            default:
                throw CommonUtil.unreachableStatement("Unrecognised tx type " + txType);
        }
    }

    private static RuntimeException convertStatusRuntimeException(StatusRuntimeException error) {
        Status status = error.getStatus();
        if (status.getCode().equals(Status.Code.UNKNOWN)) {
            String message = status.getDescription();
            return new MyGraknException(message);
        } else {
            return error;
        }
    }

    private static class MyGraknException extends GraknException {

        MyGraknException(String error) {
            super(error);
        }
    }
}
