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

package grakn.core.daemon.migrate;

import grakn.core.common.config.Config;
import grakn.core.common.config.ConfigKey;
import grakn.core.common.config.SystemProperty;
import grakn.core.server.migrate.proto.MigrateProto;
import grakn.core.server.migrate.proto.MigrateServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class MigrationClient implements AutoCloseable {

    private final ManagedChannel channel;
    private final MigrateServiceGrpc.MigrateServiceStub asyncStub;
    private final MigrateServiceGrpc.MigrateServiceBlockingStub blockingStub;
    private final boolean isLocal;

    private final Thread shutdownHook;
    private final Set<CancellableGrpcConsumer<?, ?>> jobs = Collections.synchronizedSet(new HashSet<>());

    private MigrationClient(String uri, boolean isLocal) {
        this.isLocal = isLocal;
        channel = ManagedChannelBuilder.forTarget(uri)
                .usePlaintext().build();
        asyncStub = MigrateServiceGrpc.newStub(channel);
        blockingStub = MigrateServiceGrpc.newBlockingStub(channel);

        shutdownHook = new Thread(this::shutdown);
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    public MigrationClient(String uri) {
        this(uri, false);
    }

    public MigrationClient() {
        this("localhost:" + readPort(), true);
    }

    private <T, U> void call(BiConsumer<T, StreamObserver<U>> rpc, T request, Consumer<U> observer) throws Exception {
        CancellableGrpcConsumer<T, U> job = new CancellableGrpcConsumer<>(rpc, request, observer);
        jobs.add(job);
        try {
            job.execute();
        } finally {
            jobs.remove(job);
        }
    }

    private void reportProgress(ProgressListener progressListener, MigrateProto.Job.Res res) {
        switch (res.getResCase()) {
            case PROGRESS:
                progressListener.onProgress(res.getProgress().getCurrentProgress(), res.getProgress().getTotalCount());
                break;
            case COMPLETION:
                progressListener.onCompletion(res.getCompletion().getTotalCount());
            case RES_NOT_SET:
            default:
                // Ignore unknown message
        }
    }

    public void export(String keyspace, String path, ProgressListener progressListener) throws Exception {
        String resolvedPath = resolvePath(path);

        call(
                asyncStub::exportFile,
                MigrateProto.ExportFile.Req.newBuilder()
                        .setName(keyspace)
                        .setPath(resolvedPath)
                        .build(),
                res -> reportProgress(progressListener, res)
        );
    }

    public void import_(String keyspace, String path, ProgressListener progressListener) throws Exception {
        String resolvedPath = resolvePath(path);

        call(
                asyncStub::importFile,
                MigrateProto.ImportFile.Req.newBuilder()
                        .setName(keyspace)
                        .setPath(resolvedPath)
                        .build(),
                res -> reportProgress(progressListener, res)
        );
    }

    public String exportSchema(String keyspace) {
        return blockingStub.exportSchema(
                MigrateProto.ExportSchema.Req.newBuilder()
                        .setName(keyspace)
                        .build())
                .getSchema();
    }

    private String resolvePath(String path) {
        if (isLocal) {
            return Paths.get(path).toAbsolutePath().toString();
        } else {
            return path;
        }
    }

    private void shutdown() {
        jobs.forEach(CancellableGrpcConsumer::cancel);
        channel.shutdownNow();
    }

    @Override
    public void close() throws Exception {
        shutdown();
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
    }

    private static int readPort() {
        return Config.read(Paths.get(Objects.requireNonNull(SystemProperty.CONFIGURATION_FILE.value()))).getProperty(ConfigKey.GRPC_PORT);
    }

    public interface ProgressListener {
        void onProgress(long current, long total);
        void onCompletion(long total);
    }

    private static class CancellableGrpcConsumer<T, U> implements ClientResponseObserver<T, U> {
        private final BiConsumer<T, StreamObserver<U>> rpc;
        private final T request;
        private final Consumer<U> observer;

        private final CountDownLatch finished = new CountDownLatch(1);
        private volatile Throwable error;
        private volatile boolean cancelled;
        private volatile ClientCallStreamObserver<T> requestStream;

        CancellableGrpcConsumer(BiConsumer<T, StreamObserver<U>> rpc, T request, Consumer<U> observer) {
            this.rpc = rpc;
            this.request = request;
            this.observer = observer;
        }

        void execute() throws Exception {
            rpc.accept(request, this);
            finished.await();
            if (error != null) {
                throw (Exception) error;
            }
        }

        synchronized void cancel() {
            cancelled = true;
            if (requestStream != null) {
                requestStream.cancel("Cancelled.", new StatusRuntimeException(Status.CANCELLED));
            }
        }

        @Override
        public synchronized void beforeStart(ClientCallStreamObserver<T> requestStream) {
            this.requestStream = requestStream;
            if (cancelled) {
                cancel();
            }
        }

        @Override
        public void onNext(U value) {
            observer.accept(value);
        }

        @Override
        public void onError(Throwable t) {
            error = t;
            finished.countDown();
        }

        @Override
        public void onCompleted() {

            finished.countDown();
        }
    }
}
