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

import ai.grakn.GraknTx;
import ai.grakn.engine.task.postprocessing.PostProcessor;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.GraknException;
import ai.grakn.exception.GraknServerException;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.exception.PropertyNotUniqueException;
import ai.grakn.exception.TemporaryWriteException;
import ai.grakn.grpc.GrpcOpenRequestExecutor;
import ai.grakn.grpc.GrpcUtil;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.rpc.generated.GrpcGrakn.DeleteRequest;
import ai.grakn.rpc.generated.GrpcGrakn.DeleteResponse;
import ai.grakn.rpc.generated.GrpcGrakn.TxRequest;
import ai.grakn.rpc.generated.GrpcGrakn.TxResponse;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nullable;


/**
 *  Service used by GrpcServer to provide Grakn core functionality via gRPC
 *
 *  @author marcoscoppetta
 */


public class GrpcGraknService extends GraknGrpc.GraknImplBase {

    private final GrpcOpenRequestExecutor executor;
    private PostProcessor postProcessor;

    public GrpcGraknService(GrpcOpenRequestExecutor executor, PostProcessor postProcessor) {
        this.executor = executor;
        this.postProcessor = postProcessor;
    }

    @Override
    public StreamObserver<TxRequest> tx(StreamObserver<TxResponse> responseObserver) {
        return TxObserver.create(responseObserver, executor, postProcessor);
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        try {
            runAndConvertGraknExceptions(() -> {
                try (GraknTx tx = executor.execute(request.getOpen())) {
                    tx.admin().delete();
                }

                responseObserver.onNext(GrpcUtil.deleteResponse());
                responseObserver.onCompleted();
            });
        } catch (StatusRuntimeException e) {
            responseObserver.onError(e);
        }
    }

    static void runAndConvertGraknExceptions(Runnable runnable) {
        try {
            runnable.run();
        } catch (TemporaryWriteException e) {
            throw convertGraknException(e, GrpcUtil.ErrorType.TEMPORARY_WRITE_EXCEPTION);
        } catch (GraknServerException e) {
            throw convertGraknException(e, GrpcUtil.ErrorType.GRAKN_SERVER_EXCEPTION);
        } catch (GraknBackendException e) {
            throw convertGraknException(e, GrpcUtil.ErrorType.GRAKN_BACKEND_EXCEPTION);
        } catch (PropertyNotUniqueException e) {
            throw convertGraknException(e, GrpcUtil.ErrorType.PROPERTY_NOT_UNIQUE_EXCEPTION);
        } catch (GraknTxOperationException e) {
            throw convertGraknException(e, GrpcUtil.ErrorType.GRAKN_TX_OPERATION_EXCEPTION);
        } catch (GraqlQueryException e) {
            throw convertGraknException(e, GrpcUtil.ErrorType.GRAQL_QUERY_EXCEPTION);
        } catch (GraqlSyntaxException e) {
            throw convertGraknException(e, GrpcUtil.ErrorType.GRAQL_SYNTAX_EXCEPTION);
        } catch (InvalidKBException e) {
            throw convertGraknException(e, GrpcUtil.ErrorType.INVALID_KB_EXCEPTION);
        } catch (GraknException e) {
            // We shouldn't normally encounter this case unless someone adds a new exception class
            throw convertGraknException(e, GrpcUtil.ErrorType.UNKNOWN);
        }
    }

    private static StatusRuntimeException convertGraknException(GraknException exception, GrpcUtil.ErrorType errorType) {
        Metadata trailers = new Metadata();
        trailers.put(GrpcUtil.ErrorType.KEY, errorType);
        return error(Status.UNKNOWN.withDescription(exception.getMessage()), trailers);
    }

    static StatusRuntimeException error(Status status) {
        return error(status, null);
    }

    static <T> T nonNull(@Nullable T item) {
        if (item == null) {
            throw GrpcGraknService.error(Status.FAILED_PRECONDITION);
        } else {
            return item;
        }
    }

    private static StatusRuntimeException error(Status status, @Nullable Metadata trailers) {
        return new StatusRuntimeException(status, trailers);
    }
}
