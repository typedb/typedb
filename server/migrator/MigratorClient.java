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
 *
 */

package grakn.core.server.migrator;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.server.migrator.proto.MigratorGrpc;
import grakn.core.server.migrator.proto.MigratorProto;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class MigratorClient {

    private final MigratorGrpc.MigratorStub stub;

    public MigratorClient(final int serverPort) {
        final String uri = "localhost:" + serverPort;
        final ManagedChannel channel = ManagedChannelBuilder.forTarget(uri).usePlaintext().build();
        stub = MigratorGrpc.newStub(channel);
    }

    public boolean importData(final String database, final String filename, final Map<String, String> remapLabels) {
        final MigratorProto.ImportData.Req req = MigratorProto.ImportData.Req.newBuilder()
                .setDatabase(database)
                .setFilename(filename)
                .putAllRemapLabels(remapLabels)
                .build();
        final ResponseObserver streamObserver = new ResponseObserver(new ProgressPrinter("import"));
        stub.importData(req, streamObserver);
        streamObserver.await();
        return streamObserver.success();
    }

    public boolean exportData(final String database, final String filename) {
        final MigratorProto.ExportData.Req req = MigratorProto.ExportData.Req.newBuilder()
                .setDatabase(database)
                .setFilename(filename)
                .build();
        final ResponseObserver streamObserver = new ResponseObserver(new ProgressPrinter("export"));
        stub.exportData(req, streamObserver);
        streamObserver.await();
        return streamObserver.success();
    }

    static class ResponseObserver implements StreamObserver<MigratorProto.Job.Res> {

        private final ProgressPrinter progressPrinter;
        private final CountDownLatch latch;
        private boolean success;

        public ResponseObserver(final ProgressPrinter progressPrinter) {
            this.progressPrinter = progressPrinter;
            this.latch = new CountDownLatch(1);
        }

        @Override
        public void onNext(final MigratorProto.Job.Res res) {
            final long current = res.getProgress().getCurrent();
            final long total = res.getProgress().getTotal();
            progressPrinter.onProgress(current, total);
        }

        @Override
        public void onError(final Throwable throwable) {
            throwable.printStackTrace();
            success = false;
            latch.countDown();
        }

        @Override
        public void onCompleted() {
            progressPrinter.onCompletion();
            success = true;
            latch.countDown();
        }

        public void await() {
            try {
                latch.await();
            } catch (final InterruptedException e) {
                throw GraknException.of(ErrorMessage.Internal.UNEXPECTED_INTERRUPTION);
            }
        }

        public boolean success() {
            return success;
        }
    }
}
