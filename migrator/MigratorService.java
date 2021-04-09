/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.migrator;

import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;
import grakn.core.migrator.proto.MigratorGrpc;
import grakn.core.migrator.proto.MigratorProto;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static grakn.core.common.exception.ErrorMessage.Internal.UNEXPECTED_INTERRUPTION;

public class MigratorService extends MigratorGrpc.MigratorImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(MigratorService.class);
    private final Grakn grakn;
    private final String version;

    public MigratorService(Grakn grakn, String version) {
        this.grakn = grakn;
        this.version = version;
    }

    @Override
    public void exportData(MigratorProto.ExportData.Req request, StreamObserver<MigratorProto.Job.Res> responseObserver) {
        DataExporter exporter = new DataExporter(grakn, request.getDatabase(), Paths.get(request.getFilename()), version);
        runMigrator(exporter, responseObserver);
    }

    @Override
    public void importData(MigratorProto.ImportData.Req request, StreamObserver<MigratorProto.Job.Res> responseObserver) {
        Path file = Paths.get(request.getFilename());
        DataImporter importer = new DataImporter(grakn, request.getDatabase(), file, request.getRemapLabelsMap(), version);
        runMigrator(importer, responseObserver);
    }

    private void runMigrator(Migrator migrator, StreamObserver<MigratorProto.Job.Res> responseObserver) {
        try {
            CompletableFuture<Void> migratorJob = CompletableFuture.runAsync(migrator::run);
            try {
                while (!migratorJob.isDone()) {
                    Thread.sleep(1000);
                    responseObserver.onNext(MigratorProto.Job.Res.newBuilder().setProgress(migrator.getProgress()).build());
                }
                migratorJob.get();
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

    public static StatusRuntimeException exception(Throwable e) {
        if (e instanceof StatusRuntimeException) {
            return (StatusRuntimeException) e;
        } else {
            return Status.INTERNAL.withDescription(e.getMessage() + " Please check server logs for the stack trace.").asRuntimeException();
        }
    }
}
