/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.migrator;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.diagnostics.Diagnostics;
import com.vaticle.typedb.core.migrator.database.DatabaseExporter;
import com.vaticle.typedb.core.migrator.database.DatabaseImporter;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;

public class MigratorService extends MigratorGrpc.MigratorImplBase {
    private static final Logger LOG = LoggerFactory.getLogger(MigratorService.class);

    private final TypeDB.DatabaseManager databaseMgr;
    private final String version;

    public MigratorService(TypeDB.DatabaseManager databaseMgr, String version) {
        this.databaseMgr = databaseMgr;
        this.version = version;
    }

    @Override
    public void exportDatabase(MigratorProto.Export.Req request, StreamObserver<MigratorProto.Export.Progress> responseObserver) {
        try {
            DatabaseExporter exporter = new DatabaseExporter(
                    databaseMgr, request.getDatabase(), Paths.get(request.getSchemaFile()),
                    Paths.get(request.getDataFile()), version
            );
            CompletableFuture<Void> migratorJob = CompletableFuture.runAsync(exporter::run);
            while (!migratorJob.isDone()) {
                Thread.sleep(1000);
                responseObserver.onNext(exporter.getProgress());
            }
            migratorJob.join();
            responseObserver.onCompleted();
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
            Diagnostics.get().submitError(request.getDatabase(), e);
            responseObserver.onError(exception(e));
        }
    }

    @Override
    public void importDatabase(MigratorProto.Import.Req request, StreamObserver<MigratorProto.Import.Progress> responseObserver) {
        DatabaseImporter importer = null;
        try {
            importer = new DatabaseImporter(
                    databaseMgr, request.getDatabase(), Paths.get(request.getSchemaFile()),
                    Paths.get(request.getDataFile()), version
            );
            CompletableFuture<Void> migratorJob = CompletableFuture.runAsync(importer::run);
            while (!migratorJob.isDone()) {
                Thread.sleep(1000);
                responseObserver.onNext(importer.getProgress());
            }
            migratorJob.join();
            responseObserver.onCompleted();
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
            Diagnostics.get().submitError(request.getDatabase(), e);
            responseObserver.onError(exception(e));
        } finally {
            if (importer != null) importer.close();
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
