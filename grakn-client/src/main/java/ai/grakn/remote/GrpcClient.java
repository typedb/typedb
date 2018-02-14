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
import ai.grakn.grpc.GrpcUtil;
import ai.grakn.grpc.TxGrpcCommunicator;
import ai.grakn.grpc.TxGrpcCommunicator.Response;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;
import ai.grakn.util.CommonUtil;
import com.google.common.collect.ImmutableList;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import javax.annotation.Nullable;
import java.util.List;

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
final class GrpcClient implements AutoCloseable {

    private final TxGrpcCommunicator communicator;

    private GrpcClient(TxGrpcCommunicator communicator) {
        this.communicator = communicator;
    }

    public static GrpcClient create(GraknGrpc.GraknStub stub) {
        TxGrpcCommunicator observer = TxGrpcCommunicator.create(stub);
        return new GrpcClient(observer);
    }

    public void open(Keyspace keyspace, GraknTxType txType) {
        communicator.send(GrpcUtil.openRequest(keyspace, txType));
        responseOrThrow();
    }

    public List<Object> execQuery(Query<?> query, @Nullable Boolean infer) {
        communicator.send(GrpcUtil.execQueryRequest(query.toString(), infer));

        ImmutableList.Builder<Object> results = ImmutableList.builder();

        while (true) {
            TxResponse response = responseOrThrow();

            switch (response.getResponseCase()) {
                case QUERYRESULT:
                    Object result = GrpcUtil.getQueryResult(response.getQueryResult());
                    results.add(result);
                    break;
                case DONE:
                    return results.build();
                default:
                case RESPONSE_NOT_SET:
                    throw CommonUtil.unreachableStatement("Unexpected " + response);
            }

            communicator.send(GrpcUtil.nextRequest());
        }
    }

    public void commit() {
        communicator.send(GrpcUtil.commitRequest());
        responseOrThrow();
    }

    @Override
    public void close() {
        communicator.close();
    }

    private TxResponse responseOrThrow() {
        Response response;

        try {
            response = communicator.receive();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MyGraknException("interrupt"); // TODO
        }

        switch (response.type()) {
            case OK:
                return response.ok();
            case ERROR:
                throw convertStatusRuntimeException(response.error());
            case COMPLETED:
            default:
                throw CommonUtil.unreachableStatement("Unexpected response " + response);
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
