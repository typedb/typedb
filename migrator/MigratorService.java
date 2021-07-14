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

import com.vaticle.typedb.core.migrator.data.DataExporter;
import com.vaticle.typedb.core.migrator.data.DataImporter;
import com.vaticle.typedb.core.rocks.RocksTypeDB;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;

public class MigratorService extends MigratorGrpc.MigratorImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(MigratorService.class);
    private final RocksTypeDB typedb;
    private final String version;

    public MigratorService(RocksTypeDB typedb, String version) {
        this.typedb = typedb;
        this.version = version;
    }

    @Override
    public void exportData(MigratorProto.Export.Req request, StreamObserver<MigratorProto.Export.Progress> responseObserver) {
        DataExporter exporter = new DataExporter(typedb, request.getDatabase(), Paths.get(request.getFilename()), version);
        try {
            CompletableFuture<Void> migratorJob = CompletableFuture.runAsync(exporter::run);
            while (!migratorJob.isDone()) {
                Thread.sleep(1000);
                responseObserver.onNext(exporter.getProgress());
            }
            migratorJob.get();
            responseObserver.onCompleted();
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
            responseObserver.onError(exception(e));
        }
    }

    @Override
    public void importData(MigratorProto.Import.Req request, StreamObserver<MigratorProto.Import.Progress> responseObserver) {
        DataImporter importer = new DataImporter(typedb, request.getDatabase(), Paths.get(request.getFilename()),
                request.getRemapLabelsMap(), version);
        try {
            CompletableFuture<Void> migratorJob = CompletableFuture.runAsync(importer::run);
            while (!migratorJob.isDone()) {
                Thread.sleep(1000);
                responseObserver.onNext(importer.getProgress());
            }
            migratorJob.get();
            responseObserver.onCompleted();
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
            responseObserver.onError(exception(e));
        } finally {
            importer.close();
        }
    }

    public static StatusRuntimeException exception(Throwable e) {
        if (e instanceof StatusRuntimeException) {
            return (StatusRuntimeException) e;
        } else {
            return Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException();
        }
    }
}
