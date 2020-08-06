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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import grakn.core.kb.server.Session;
import grakn.core.server.keyspace.KeyspaceImpl;
import grakn.core.server.migrate.AbstractJob;
import grakn.core.server.migrate.Export;
import grakn.core.server.migrate.Import;
import grakn.core.server.migrate.Output;
import grakn.core.server.migrate.OutputFile;
import grakn.core.server.migrate.Schema;
import grakn.core.server.migrate.proto.DataProto;
import grakn.core.server.migrate.proto.MigrateProto;
import grakn.core.server.migrate.proto.MigrateServiceGrpc;
import grakn.core.server.session.SessionFactory;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MigrateService extends MigrateServiceGrpc.MigrateServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(MigrateService.class);
    private final SessionFactory sessionFactory;

    private final ExecutorService migrationExecutor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("migration-executor-thread").build());

    public MigrateService(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;

        Runtime.getRuntime().addShutdownHook(new Thread(migrationExecutor::shutdownNow));
    }

    @Override
    public void exportFile(MigrateProto.ExportFile.Req request,
                           StreamObserver<MigrateProto.Job.Res> responseObserver) {
        Path outputPath = Paths.get(request.getPath());

        try (OutputFile output = new OutputFile(outputPath)){
            runJob(new Export(sessionFactory, output, request.getName()), responseObserver);
        } catch (IOException e) {
            handleError(responseObserver, e);
        }
    }

    @Override
    public void exportStream(MigrateProto.ExportStream.Req request,
                           StreamObserver<DataProto.Item> responseObserver) {

        Export export = new Export(sessionFactory, new Output() {
            @Override
            public synchronized void write(DataProto.Item item) {
                responseObserver.onNext(item);
            }
        }, request.getName());

        ServerCallStreamObserver<DataProto.Item> serverCallStreamObserver = (ServerCallStreamObserver<DataProto.Item>) responseObserver;

        serverCallStreamObserver.setOnCancelHandler(export::cancel);

        try {
            export.execute();
        } catch (Exception e) {
            handleError(responseObserver, e);
        }
    }

    @Override
    public void importFile(MigrateProto.ImportFile.Req request,
                           StreamObserver<MigrateProto.Job.Res> responseObserver) {
        Path inputPath = Paths.get(request.getPath());
        Map<String, String> remapLabels = request.getRemapLabelsMap();

        runJob(new Import(sessionFactory, inputPath, request.getName(), remapLabels), responseObserver);
    }

    @Override
    public void exportSchema(MigrateProto.ExportSchema.Req request,
                                 StreamObserver<MigrateProto.ExportSchema.Res> responseObserver) {

        try (Session session = sessionFactory.session(new KeyspaceImpl(request.getName()))) {
            StringWriter writer = new StringWriter();
            Schema schema = new Schema(session);
            schema.printSchema(writer);

            responseObserver.onNext(MigrateProto.ExportSchema.Res.newBuilder().setSchema(writer.toString()).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOG.error("An error occurred during schema export.", e);
            handleError(responseObserver, e);
        }
    }

    private void runJob(AbstractJob job, StreamObserver<MigrateProto.Job.Res> responseObserver) {
        try {
            migrationExecutor.execute(job::execute);

            while (!job.awaitCompletion(1, TimeUnit.SECONDS)) {
                responseObserver.onNext(MigrateProto.Job.Res.newBuilder()
                        .setProgress(job.getCurrentProgress())
                        .build());
            }

            responseObserver.onNext(MigrateProto.Job.Res.newBuilder()
                    .setCompletion(job.getCompletion())
                    .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOG.error(String.format("An error occurred during %s.", job.getName()), e);
            handleError(responseObserver, e);
            job.cancel();
        }
    }

    private <T> void handleError(StreamObserver<T> observer, Throwable e) {
        observer.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
}
