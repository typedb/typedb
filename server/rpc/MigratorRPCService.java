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
import grakn.core.server.migrator.Exporter;
import grakn.core.server.migrator.Importer;
import grakn.core.server.migrator.Migrator;
import grakn.core.server.migrator.Schema;
import grakn.core.server.migrator.proto.MigratorGrpc;
import grakn.core.server.migrator.proto.MigratorProto;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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
        Exporter exporter = new Exporter(grakn, request.getDatabase(), Paths.get(request.getFilename()));
        runMigrator(exporter, responseObserver);
    }

    @Override
    public void importData(MigratorProto.ImportData.Req request, StreamObserver<MigratorProto.Job.Res> responseObserver) {
        Importer importer = new Importer(grakn, request.getDatabase(), Paths.get(request.getFilename()), request.getRemapLabelsMap());
        runMigrator(importer, responseObserver);
    }

    @Override
    public void getSchema(MigratorProto.GetSchema.Req request, StreamObserver<MigratorProto.GetSchema.Res> responseObserver) {
        try {
            String schema = new Schema(grakn, request.getDatabase()).getSchema();
            MigratorProto.GetSchema.Res res = MigratorProto.GetSchema.Res.newBuilder().setSchema(schema).build();
            responseObserver.onNext(res);
            responseObserver.onCompleted();
        } catch (GraknException e) {
            LOG.error(e.getMessage(), e);
            responseObserver.onError(exception(e));
        }
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
}
