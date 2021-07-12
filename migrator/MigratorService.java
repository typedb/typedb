/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.migrator;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.migrator.proto.MigratorGrpc;
import com.vaticle.typedb.core.migrator.proto.MigratorProto;
import com.vaticle.typedb.core.rocks.RocksTypeDB;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNEXPECTED_INTERRUPTION;

public class MigratorService extends MigratorGrpc.MigratorImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(MigratorService.class);
    private final RocksTypeDB typedb;
    private final String version;

    public MigratorService(RocksTypeDB typedb, String version) {
        this.typedb = typedb;
        this.version = version;
    }

    @Override
    public void exportData(MigratorProto.ExportData.Req request, StreamObserver<MigratorProto.Job.Res> responseObserver) {
        DataExporter exporter = new DataExporter(typedb, request.getDatabase(), Paths.get(request.getFilename()), version);
        runMigrator(exporter, responseObserver);
    }

    @Override
    public void importData(MigratorProto.ImportData.Req request, StreamObserver<MigratorProto.Job.Res> responseObserver) {
        Path file = Paths.get(request.getFilename());
        DataImporter importer = new DataImporter(typedb, request.getDatabase(), file, request.getRemapLabelsMap(), version);
        runMigrator(importer, responseObserver);
    }

    private void runMigrator(Migrator migrator, StreamObserver<MigratorProto.Job.Res> responseObserver) {
        try {
            CompletableFuture<Void> migratorJob = CompletableFuture.runAsync(migrator::run);
            while (!migratorJob.isDone()) {
                Thread.sleep(1000);
                responseObserver.onNext(MigratorProto.Job.Res.newBuilder().setProgress(migrator.getProgress()).build());
            }
            migratorJob.get();
            responseObserver.onCompleted();
        } catch (InterruptedException | ExecutionException e) {
            throw TypeDBException.of(UNEXPECTED_INTERRUPTION);
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
            responseObserver.onError(exception(e));
        } finally {
            migrator.close();
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
