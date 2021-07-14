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

    private final MigratorGrpc.MigratorStub stub;

    public MigratorClient(int serverPort) {
        String uri = "localhost:" + serverPort;
        ManagedChannel channel = ManagedChannelBuilder.forTarget(uri).usePlaintext().build();
        stub = MigratorGrpc.newStub(channel);
    }

    public boolean importData(String database, String filename, Map<String, String> remapLabels) {
        MigratorProto.Import.Req req = MigratorProto.Import.Req.newBuilder()
                .setDatabase(database)
                .setFilename(filename)
                .putAllRemapLabels(remapLabels)
                .build();
        ResponseObserver.Import streamObserver = new ResponseObserver.Import(new ProgressPrinter.Import());
        stub.importData(req, streamObserver);
        streamObserver.await();
        return streamObserver.success();
    }

    public boolean exportData(String database, String filename) {
        MigratorProto.Export.Req req = MigratorProto.Export.Req.newBuilder()
                .setDatabase(database)
                .setFilename(filename)
                .build();
        ResponseObserver.Export streamObserver = new ResponseObserver.Export(new ProgressPrinter.Export());
        stub.exportData(req, streamObserver);
        streamObserver.await();
        return streamObserver.success();
    }

    static abstract class ResponseObserver<T> implements StreamObserver<T> {

        private final CountDownLatch latch;
        private boolean success;

        public ResponseObserver() {
            this.latch = new CountDownLatch(1);
        }

        private static class Import extends ResponseObserver<MigratorProto.Import.Progress> {

            private final ProgressPrinter.Import progressPrinter;

            public Import(ProgressPrinter.Import progressPrinter) {
                super();
                this.progressPrinter = progressPrinter;
            }

            @Override
            public void onNext(MigratorProto.Import.Progress progress) {
                progressPrinter.onProgress(progress);
            }

            @Override
            public void onCompleted() {
                super.onCompleted();
                progressPrinter.onCompleted();
            }
        }

        private static class Export extends ResponseObserver<MigratorProto.Export.Progress> {

            private final ProgressPrinter.Export progressPrinter;

            public Export(ProgressPrinter.Export progressPrinter) {
                super();
                this.progressPrinter = progressPrinter;
            }

            @Override
            public void onNext(MigratorProto.Export.Progress progress) {
                progressPrinter.onProgress(progress);
            }

            @Override
            public void onCompleted() {
                super.onCompleted();
                progressPrinter.onCompleted();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            throwable.printStackTrace();
            success = false;
            latch.countDown();
        }

        @Override
        public void onCompleted() {
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

    private static abstract class ProgressPrinter {

        private static final String[] ANIM = new String[]{"-", "\\", "|", "/"};
        private static final String STATUS_STARTING = "starting";
        private static final String STATUS_IN_PROGRESS = "in progress";
        private static final String STATUS_COMPLETED = "completed";

        private final Timer timer = new Timer();

        String status = STATUS_STARTING;

        private int anim = 0;
        private int lines = 0;

        public ProgressPrinter() {
            TimerTask task = new TimerTask() {
                @Override
                public void run() {
                    step();
                }
            };
            timer.scheduleAtFixedRate(task, 0, 100);
        }

        public void onCompleted() {
            status = STATUS_COMPLETED;
            step();
            timer.cancel();
        }

        abstract String type();

        abstract String formattedProgress();

        private synchronized void step() {
            StringBuilder builder = new StringBuilder();
            builder.append(String.format("$x isa %s,\n    has status \"%s\";", type(), status));

            if (!status.equals(STATUS_STARTING)) {
                builder.append("\n\n");
                builder.append(formattedProgress());
                builder.append(";");
                anim = (anim + 1) % ANIM.length;
                builder.append(" ").append(ANIM[anim]);
            }

            String output = builder.toString();
            System.out.println((lines > 0 ? "\033[" + lines + "F\033[J" : "") + output);

            lines = output.split("\n").length;
        }

        private static class Import extends ProgressPrinter {

            private MigratorProto.Import.Progress prog;

            void onProgress(MigratorProto.Import.Progress progress) {
                this.status = STATUS_IN_PROGRESS;
                this.prog = progress;
            }

            @Override
            String formattedProgress() {
                StringBuilder progressStr = new StringBuilder();
                progressStr.append(prog.getAttributes() == 0 ?
                        String.format("Attribute: %d", prog.getAttributesCurrent()) :
                        String.format("Attribute: %d/%d (%.1f%%)", prog.getAttributesCurrent(), prog.getAttributes(),
                                100.0 * prog.getAttributesCurrent() / prog.getAttributes()));
                progressStr.append("\n");
                progressStr.append(prog.getEntities() == 0 ?
                        String.format("Entity: %d", prog.getEntitiesCurrent()) :
                        String.format("Entity: %d/%d (%.1f%%)", prog.getEntitiesCurrent(), prog.getEntities(),
                                100.0 * prog.getEntitiesCurrent() / prog.getEntities()));
                progressStr.append("\n");
                progressStr.append(prog.getRelations() == 0 ?
                        String.format("Relation: %d", prog.getRelationsCurrent()) :
                        String.format("Relation: %d/%d (%.1f%%)", prog.getRelationsCurrent(), prog.getRelations(),
                                100.0 * prog.getRelationsCurrent() / prog.getRelations()));
                progressStr.append("\n");
                long currentThings = prog.getAttributesCurrent() + prog.getEntitiesCurrent() + prog.getRelationsCurrent();
                long things = prog.getAttributes() + prog.getEntities() + prog.getRelations();
                progressStr.append("\n");
                progressStr.append(String.format("Total: %d/%d (%.1f%%)", currentThings, things, 100.0 * currentThings / things));
                return progressStr.toString();
            }

            @Override
            String type() {
                return "Import";
            }
        }

        private static class Export extends ProgressPrinter {

            private MigratorProto.Export.Progress prog;

            void onProgress(MigratorProto.Export.Progress progress) {
                this.status = STATUS_IN_PROGRESS;
                this.prog = progress;
            }

            @Override
            String formattedProgress() {
                StringBuilder progressStr = new StringBuilder();
                progressStr.append(prog.getAttributes() == 0 ?
                        String.format("Attribute: %d", prog.getAttributesCurrent()) :
                        String.format("Attribute: %d/%d (%.1f%%)", prog.getAttributesCurrent(), prog.getAttributes(),
                                100.0 * prog.getAttributesCurrent() / prog.getAttributes()));
                progressStr.append("\n");
                progressStr.append(prog.getEntities() == 0 ?
                        String.format("Entity: %d", prog.getEntitiesCurrent()) :
                        String.format("Entity: %d/%d (%.1f%%)", prog.getEntitiesCurrent(), prog.getEntities(),
                                100.0 * prog.getEntitiesCurrent() / prog.getEntities()));
                progressStr.append("\n");
                progressStr.append(prog.getRelations() == 0 ?
                        String.format("Relation: %d", prog.getRelationsCurrent()) :
                        String.format("Relation: %d/%d (%.1f%%)", prog.getRelationsCurrent(), prog.getRelations(),
                                100.0 * prog.getRelationsCurrent() / prog.getRelations()));
                progressStr.append("\n");
                long currentThings = prog.getAttributesCurrent() + prog.getEntitiesCurrent() + prog.getRelationsCurrent();
                long things = prog.getAttributes() + prog.getEntities() + prog.getRelations();
                progressStr.append("\n");
                progressStr.append(String.format("Total: %d/%d (%.1f%%)", currentThings, things, 100.0 * currentThings / things));
                return progressStr.toString();
            }

            @Override
            String type() {
                return "Export";
            }
        }
    }
}
