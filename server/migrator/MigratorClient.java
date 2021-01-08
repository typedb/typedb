/*
 * Copyright (C) 2021 Grakn Labs
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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

public class MigratorClient {

    private final MigratorGrpc.MigratorStub streamingStub;
    private final MigratorGrpc.MigratorBlockingStub blockingStub;

    public MigratorClient(final int serverPort) {
        final String uri = "localhost:" + serverPort;
        final ManagedChannel channel = ManagedChannelBuilder.forTarget(uri).usePlaintext().build();
        streamingStub = MigratorGrpc.newStub(channel);
        blockingStub = MigratorGrpc.newBlockingStub(channel);
    }

    public boolean importData(final String database, final String filename, final Map<String, String> remapLabels) {
        final MigratorProto.ImportData.Req req = MigratorProto.ImportData.Req.newBuilder()
                .setDatabase(database)
                .setFilename(filename)
                .putAllRemapLabels(remapLabels)
                .build();
        final ResponseObserver streamObserver = new ResponseObserver(new ProgressPrinter("import"));
        streamingStub.importData(req, streamObserver);
        streamObserver.await();
        return streamObserver.success();
    }

    public boolean exportData(final String database, final String filename) {
        final MigratorProto.ExportData.Req req = MigratorProto.ExportData.Req.newBuilder()
                .setDatabase(database)
                .setFilename(filename)
                .build();
        final ResponseObserver streamObserver = new ResponseObserver(new ProgressPrinter("export"));
        streamingStub.exportData(req, streamObserver);
        streamObserver.await();
        return streamObserver.success();
    }

    public void printSchema(final String database) {
        final MigratorProto.GetSchema.Req req = MigratorProto.GetSchema.Req.newBuilder()
                .setDatabase(database)
                .build();
        MigratorProto.GetSchema.Res res = blockingStub.getSchema(req);
        System.out.println(res.getSchema());
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

    private static class ProgressPrinter {

        private static final String[] ANIM = new String[]{
                "-",
                "\\",
                "|",
                "/"
        };
        private static final String STATUS_STARTING = "starting";
        private static final String STATUS_IN_PROGRESS = "in progress";
        private static final String STATUS_COMPLETED = "completed";

        private final String type;
        private final Timer timer = new Timer();

        private String status = STATUS_STARTING;
        private long current = 0;
        private long total = 0;

        private int anim = 0;
        private int lines = 0;

        public ProgressPrinter(final String type) {
            this.type = type;
            final TimerTask task = new TimerTask() {
                @Override
                public void run() {
                    step();
                }
            };
            timer.scheduleAtFixedRate(task, 0, 100);
        }

        public void onProgress(final long current, final long total) {
            status = STATUS_IN_PROGRESS;
            this.current = current;
            this.total = total;
        }

        public void onCompletion() {
            status = STATUS_COMPLETED;
            step();
            timer.cancel();
        }

        private synchronized void step() {
            final StringBuilder builder = new StringBuilder();
            builder.append(String.format("$x isa %s,\n    has status \"%s\"", type, status));

            if (status.equals(STATUS_IN_PROGRESS)) {
                final String percent;
                final String count;
                if (total > 0) {
                    percent = String.format("%.1f%%", (double) current / (double) total * 100.0);
                    count = String.format("%,d / %,d", current, total);
                } else {
                    percent = "?";
                    count = String.format("%,d", current);
                }
                builder.append(String.format(",\n    has progress (%s),\n    has count (%s)",
                                             percent, count));
            }

            builder.append(";");
            if (status.equals(STATUS_IN_PROGRESS)) {
                anim = (anim + 1) % ANIM.length;
                builder.append(" ").append(ANIM[anim]);
            }

            final String output = builder.toString();
            System.out.println((lines > 0 ? "\033[" + lines + "F\033[J" : "") + output);

            lines = output.split("\n").length;
        }
    }
}
