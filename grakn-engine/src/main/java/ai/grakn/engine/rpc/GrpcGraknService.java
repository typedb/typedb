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

package ai.grakn.engine.rpc;

import ai.grakn.engine.task.postprocessing.PostProcessor;
import ai.grakn.grpc.GrpcOpenRequestExecutor;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.rpc.generated.GraknOuterClass;
import io.grpc.stub.StreamObserver;


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
    public StreamObserver<GraknOuterClass.TxRequest> tx(StreamObserver<GraknOuterClass.TxResponse> responseObserver) {
        return TxObserver.create(responseObserver, executor, postProcessor);
    }
}
