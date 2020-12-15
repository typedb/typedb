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

    public MigratorClient(int serverPort) {
        String uri = "localhost:" + serverPort;
        ManagedChannel channel = ManagedChannelBuilder.forTarget(uri).usePlaintext().build();
        stub = MigratorGrpc.newStub(channel);
    }

    public boolean importData(String database, String filename, Map<String, String> remapLabels) {
        MigratorProto.ImportData.Req req = MigratorProto.ImportData.Req.newBuilder()
                .setDatabase(database)
                .setFilename(filename)
                .putAllRemapLabels(remapLabels)
                .build();
        CountDownLatch latch = new CountDownLatch(1);
        ProgressPrinter progressPrinter = new ProgressPrinter("import");
        ResponseObserver streamObserver = new ResponseObserver(progressPrinter, latch);
        stub.importData(req, streamObserver);
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw GraknException.of(ErrorMessage.Internal.UNEXPECTED_INTERRUPTION);
        }
        return streamObserver.success();
    }

    public boolean exportData(String database, String filename) {
        MigratorProto.ExportData.Req req = MigratorProto.ExportData.Req.newBuilder()
                .setDatabase(database)
                .setFilename(filename)
                .build();
        CountDownLatch latch = new CountDownLatch(1);
        ProgressPrinter progressPrinter = new ProgressPrinter("export");
        ResponseObserver streamObserver = new ResponseObserver(progressPrinter, latch);
        stub.exportData(req, streamObserver);
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw GraknException.of(ErrorMessage.Internal.UNEXPECTED_INTERRUPTION);
        }
        return streamObserver.success();
    }

    static class ResponseObserver implements StreamObserver<MigratorProto.Job.Res> {

        private final ProgressPrinter progressPrinter;
        private final CountDownLatch latch;
        private boolean success;

        public ResponseObserver(ProgressPrinter progressPrinter, CountDownLatch latch) {
            this.progressPrinter = progressPrinter;
            this.latch = latch;
        }

        @Override
        public void onNext(MigratorProto.Job.Res res) {
            long current = res.getProgress().getCurrent();
            long total = res.getProgress().getTotal();
            progressPrinter.onProgress(current, total);
        }

        @Override
        public void onError(Throwable throwable) {
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

        public boolean success() {
            return success;
        }
    }
}
