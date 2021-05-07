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
 *
 */

package com.vaticle.typedb.core.migrator;

import com.vaticle.typedb.core.common.exception.ErrorMessage;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.migrator.proto.MigratorGrpc;
import com.vaticle.typedb.core.migrator.proto.MigratorProto;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

// TODO: This class does not belong in the server, it should be moved to client-side,
//       and it should be able to stream import/export file to/from the server
public class MigratorClient {

    private final MigratorGrpc.MigratorStub streamingStub;

    public MigratorClient(int serverPort) {
        String uri = "localhost:" + serverPort;
        ManagedChannel channel = ManagedChannelBuilder.forTarget(uri).usePlaintext().build();
        streamingStub = MigratorGrpc.newStub(channel);
    }

    public boolean importData(String database, String filename, Map<String, String> remapLabels) {
        MigratorProto.ImportData.Req req = MigratorProto.ImportData.Req.newBuilder()
                .setDatabase(database)
                .setFilename(filename)
                .putAllRemapLabels(remapLabels)
                .build();
        ResponseObserver streamObserver = new ResponseObserver(new ProgressPrinter("import"));
        streamingStub.importData(req, streamObserver);
        streamObserver.await();
        return streamObserver.success();
    }

    public boolean exportData(String database, String filename) {
        MigratorProto.ExportData.Req req = MigratorProto.ExportData.Req.newBuilder()
                .setDatabase(database)
                .setFilename(filename)
                .build();
        ResponseObserver streamObserver = new ResponseObserver(new ProgressPrinter("export"));
        streamingStub.exportData(req, streamObserver);
        streamObserver.await();
        return streamObserver.success();
    }

    static class ResponseObserver implements StreamObserver<MigratorProto.Job.Res> {

        private final ProgressPrinter progressPrinter;
        private final CountDownLatch latch;
        private boolean success;

        public ResponseObserver(ProgressPrinter progressPrinter) {
            this.progressPrinter = progressPrinter;
            this.latch = new CountDownLatch(1);
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

        public void await() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw TypeDBException.of(ErrorMessage.Internal.UNEXPECTED_INTERRUPTION);
            }
        }

        public boolean success() {
            return success;
        }
    }

    private static class ProgressPrinter {

        private static final String[] ANIM = new String[]{"-", "\\", "|", "/"};
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

        public ProgressPrinter(String type) {
            this.type = type;
            TimerTask task = new TimerTask() {
                @Override
                public void run() {
                    step();
                }
            };
            timer.scheduleAtFixedRate(task, 0, 100);
        }

        public void onProgress(long current, long total) {
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
            StringBuilder builder = new StringBuilder();
            builder.append(String.format("$x isa %s,\n    has status \"%s\"", type, status));

            if (status.equals(STATUS_IN_PROGRESS)) {
                String percent;
                String count;
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

            String output = builder.toString();
            System.out.println((lines > 0 ? "\033[" + lines + "F\033[J" : "") + output);

            lines = output.split("\n").length;
        }
    }
}
