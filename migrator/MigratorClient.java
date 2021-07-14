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
            progressPrinter.onProgress(ProgressPrinter.Progress.of(res.getProgress()));
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

        private Progress progress;

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

        public void onProgress(Progress progress) {
            status = STATUS_IN_PROGRESS;
            this.progress = progress;
        }

        public void onCompletion() {
            status = STATUS_COMPLETED;
            step();
            timer.cancel();
        }

        private synchronized void step() {
            StringBuilder builder = new StringBuilder();
            builder.append(String.format("$x isa %s,\n    has status \"%s\";", type, status));

            if (!status.equals(STATUS_STARTING)) {
                builder.append("\n\n");
                builder.append(progress.toString());
                builder.append(";");
                anim = (anim + 1) % ANIM.length;
                builder.append(" ").append(ANIM[anim]);
            }

            String output = builder.toString();
            System.out.println((lines > 0 ? "\033[" + lines + "F\033[J" : "") + output);

            lines = output.split("\n").length;
        }


        static abstract class Progress {

            public static Progress of(MigratorProto.Job.Progress progress) {
                if (progress.hasExportProgress()) return Export.of(progress.getExportProgress());
                else return Import.of(progress.getImportProgress());
            }

            static class Import extends Progress {

                private final MigratorProto.Job.ImportProgress prog;

                private Import(MigratorProto.Job.ImportProgress progress) {
                    this.prog = progress;
                }

                static Import of(MigratorProto.Job.ImportProgress importProgress) {
                    return new Import(importProgress);
                }

                @Override
                public String toString() {
                    StringBuilder progressStr = new StringBuilder();
                    progressStr.append(prog.getAttributes() == 0 ? String.format("Attribute: %d", prog.getAttributesCurrent()) :
                            String.format("Attribute: %d/%d (%.1f%%)", prog.getAttributesCurrent(), prog.getAttributes(), 100.0 * prog.getAttributesCurrent() / prog.getAttributes()));
                    long currentThings = prog.getAttributesCurrent();
                    long things = prog.getAttributes();
                    if (!prog.getInitialising()) {
                        progressStr.append(" ");
                        progressStr.append(prog.getOwnerships() == 0 ? String.format("[ ownership: %d/%d ]", prog.getOwnershipsCurrent(), prog.getOwnerships()) :
                                String.format("[ ownership: %d/%d (%.1f%%) ]", prog.getOwnershipsCurrent(), prog.getOwnerships(), 100.0 * prog.getOwnershipsCurrent() / prog.getOwnerships()));
                        progressStr.append("\n");
                        progressStr.append(prog.getEntities() == 0 ? String.format("Entity: %d/%d", prog.getEntitiesCurrent(), prog.getEntities()) :
                                String.format("Entity: %d/%d (%.1f%%)", prog.getEntitiesCurrent(), prog.getEntities(), 100.0 * prog.getEntitiesCurrent() / prog.getEntities()));
                        progressStr.append("\n");
                        progressStr.append(prog.getRelations() == 0 ? String.format("Relation: %d/%d", prog.getRelationsCurrent(), prog.getRelations()) :
                                String.format("Relation: %d/%d (%.1f%%)", prog.getRelationsCurrent(), prog.getRelations(), 100.0 * prog.getRelationsCurrent() / prog.getRelations()));
                        progressStr.append(" ");
                        progressStr.append(prog.getRoles() == 0 ? String.format("[ role: %d/%d ]", prog.getRolesCurrent(), prog.getRoles()) :
                                String.format("[ role: %d/%d (%.1f%%) ]", prog.getRolesCurrent(), prog.getRoles(), 100.0 * prog.getRolesCurrent() / prog.getRoles()));
                        currentThings += prog.getEntitiesCurrent() + prog.getRelationsCurrent() + prog.getOwnershipsCurrent() + prog.getRolesCurrent();
                        things += prog.getEntities() + prog.getRelations() + prog.getOwnerships() + prog.getRoles();

                        progressStr.append("\n");
                        progressStr.append(String.format("Total: %d/%d (%.1f%%)", currentThings, things, 100.0 * currentThings / things));
                    }
                    return progressStr.toString();
                }
            }

            static class Export extends Progress {

                private final MigratorProto.Job.ExportProgress prog;

                public Export(MigratorProto.Job.ExportProgress exportProgress) {
                    this.prog = exportProgress;
                }

                static Export of(MigratorProto.Job.ExportProgress exportProgress) {
                    return new Export(exportProgress);
                }

                @Override
                public String toString() {
                    StringBuilder progressStr = new StringBuilder();
                    progressStr.append(prog.getAttributes() == 0 ? String.format("Attribute: %dd", prog.getAttributesCurrent(), prog.getAttributes()) :
                            String.format("Attribute: %d/%d (%.1f%%)", prog.getAttributesCurrent(), prog.getAttributes(), 100.0 * prog.getAttributesCurrent() / prog.getAttributes()));
                    progressStr.append("\n");
                    progressStr.append(prog.getEntities() == 0 ? String.format("Entity: %dd", prog.getEntitiesCurrent(), prog.getEntities()) :
                            String.format("Entity: %d/%d (%.1f%%)", prog.getEntitiesCurrent(), prog.getEntities(), 100.0 * prog.getEntitiesCurrent() / prog.getEntities()));
                    progressStr.append("\n");
                    progressStr.append(prog.getRelations() == 0 ? String.format("Relation: %d", prog.getRelationsCurrent(), prog.getRelations()) :
                            String.format("Relation: %d/%d (%.1f%%)", prog.getRelationsCurrent(), prog.getRelations(), 100.0 * prog.getRelationsCurrent() / prog.getRelations()));
                    progressStr.append("\n");
                    long currentThings = prog.getAttributesCurrent() + prog.getEntitiesCurrent() + prog.getRelationsCurrent();
                    long things = prog.getAttributes() + prog.getEntities() + prog.getRelations();
                    progressStr.append("\n");
                    progressStr.append(String.format("\nTotal: %d/%d (%.1f%%)", currentThings, things, 100.0 * currentThings / things));
                    return progressStr.toString();
                }
            }
        }
    }
}
