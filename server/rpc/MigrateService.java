package grakn.core.server.rpc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import grakn.core.kb.server.Session;
import grakn.core.server.keyspace.KeyspaceImpl;
import grakn.core.server.migrate.AbstractJob;
import grakn.core.server.migrate.Export;
import grakn.core.server.migrate.Import;
import grakn.core.server.migrate.Schema;
import grakn.core.server.migrate.proto.MigrateProto;
import grakn.core.server.migrate.proto.MigrateServiceGrpc;
import grakn.core.server.session.SessionFactory;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
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
        Path output = Paths.get(request.getPath());

        runJob(new Export(sessionFactory, output, request.getName()), responseObserver);
    }

    @Override
    public void importFile(MigrateProto.ImportFile.Req request,
                           StreamObserver<MigrateProto.Job.Res> responseObserver) {
        Path input = Paths.get(request.getPath());

        runJob(new Import(sessionFactory, input, request.getName()), responseObserver);
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
