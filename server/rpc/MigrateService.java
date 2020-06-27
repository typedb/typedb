package grakn.core.server.rpc;

import grakn.core.kb.server.Session;
import grakn.core.server.keyspace.KeyspaceImpl;
import grakn.core.server.migrate.Export;
import grakn.core.server.migrate.Import;
import grakn.core.server.migrate.proto.MigrateProto;
import grakn.core.server.migrate.proto.MigrateServiceGrpc;
import grakn.core.server.session.SessionFactory;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class MigrateService extends MigrateServiceGrpc.MigrateServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(MigrateService.class);
    private final SessionFactory sessionFactory;

    public MigrateService(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @Override
    public void exportFile(MigrateProto.ExportFile.Req request,
                           StreamObserver<MigrateProto.ExportFile.Res> responseObserver) {
        Path output = Paths.get(request.getPath());

        try (Session session = sessionFactory.session(new KeyspaceImpl(request.getName()));
             Export export = new Export(session, output)) {
            export.export();
            responseObserver.onNext(MigrateProto.ExportFile.Res.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (IOException e) {
            LOG.error("An error occurred during export testing.", e);
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED));
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
        } catch (IOException e) {
            LOG.error("An error occurred during export testing.", e);
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED));
        }
    }
}
