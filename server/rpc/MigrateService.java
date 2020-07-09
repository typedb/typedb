package grakn.core.server.rpc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import grakn.core.kb.server.Session;
import grakn.core.server.keyspace.KeyspaceImpl;
import grakn.core.server.migrate.Export;
import grakn.core.server.migrate.Import;
import grakn.core.server.migrate.Schema;
import grakn.core.server.migrate.proto.MigrateProto;
import grakn.core.server.migrate.proto.MigrateServiceGrpc;
import grakn.core.server.session.SessionFactory;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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
                           StreamObserver<MigrateProto.ExportFile.Res> responseObserver) {
        Path output = Paths.get(request.getPath());

        Export export = new Export(sessionFactory, output, request.getName());
        try {
            migrationExecutor.execute(export::execute);

            while (!export.awaitCompletion(1, TimeUnit.SECONDS)) {
                responseObserver.onNext(MigrateProto.ExportFile.Res.newBuilder()
                        .setProgress(export.getCurrentProgress())
                        .build());
            }

            responseObserver.onCompleted();
        } catch (Exception e) {
            LOG.error("An error occurred during export testing.", e);
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED));
            export.cancel();
        }
    }

    @Override
    public void importFile(MigrateProto.ImportFile.Req request,
                           StreamObserver<MigrateProto.ImportFile.Res> responseObserver) {
        Path input = Paths.get(request.getPath());

        try (Session session = sessionFactory.session(new KeyspaceImpl(request.getName()));
             Import anImport = new Import(session, input)) {
            anImport.execute();
            responseObserver.onNext(MigrateProto.ImportFile.Res.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOG.error("An error occurred during export testing.", e);
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED));
        }
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
            LOG.error("An error occurred during export testing.", e);
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED));
        }
    }
}
