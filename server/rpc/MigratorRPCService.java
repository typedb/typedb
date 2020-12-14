/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.server.rpc;

import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;
import grakn.core.server.migrator.Importer;
import grakn.core.server.migrator.proto.MigratorGrpc;
import grakn.core.server.migrator.proto.MigratorProto;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static grakn.core.common.exception.ErrorMessage.Internal.UNEXPECTED_INTERRUPTION;
import static grakn.core.server.rpc.util.ResponseBuilder.exception;

public class MigratorRPCService extends MigratorGrpc.MigratorImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(MigratorRPCService.class);
    private final Grakn grakn;

    public MigratorRPCService(Grakn grakn) {
        this.grakn = grakn;
    }

    @Override
    public void exportData(MigratorProto.ExportData.Req request, StreamObserver<MigratorProto.Job.Res> responseObserver) {
        responseObserver.onError(exception(new UnsupportedOperationException("Export data is not currently supported.")));
    }

    @Override
    public void importData(MigratorProto.ImportData.Req request, StreamObserver<MigratorProto.Job.Res> responseObserver) {
        try {
            Importer importer = new Importer(grakn, request.getDatabase(), Paths.get(request.getFilename()), request.getRemapLabelsMap());
            CompletableFuture<Void> importerJob = CompletableFuture.runAsync(importer::run);
            try {
                while (true) {
                    // NOTE: We need to have this try...catch block since the CompletableFuture reports the progress report timeout as an exception
                    try {
                        importerJob.get(1, TimeUnit.SECONDS);
                        break;
                    } catch (TimeoutException e) {
                        responseObserver.onNext(MigratorProto.Job.Res.newBuilder().setProgress(importer.getProgress()).build());
                    }
                }
            } catch (InterruptedException e) {
                throw GraknException.of(UNEXPECTED_INTERRUPTION);
            } catch (ExecutionException e) {
                throw e.getCause();
            }
            responseObserver.onCompleted();
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
            responseObserver.onError(exception(e));
        }
    }

    @Override
    public void exportSchema(MigratorProto.ExportSchema.Req request, StreamObserver<MigratorProto.ExportSchema.Res> responseObserver) {
        responseObserver.onError(exception(new UnsupportedOperationException("Export schema is not currently supported.")));
    }
}
