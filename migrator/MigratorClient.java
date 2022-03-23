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

import java.nio.file.Path;
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

    public boolean importData(String database, Path file) {
        MigratorProto.Import.Req req = MigratorProto.Import.Req.newBuilder()
                .setDatabase(database)
                .setFilename(file.toAbsolutePath().toString())
                .build();
        ResponseObserver.Import streamObserver = new ResponseObserver.Import(new ProgressPrinter.Import());
        stub.importData(req, streamObserver);
        streamObserver.await();
        return streamObserver.success();
    }

    public boolean exportData(String database, Path file) {
        MigratorProto.Export.Req req = MigratorProto.Export.Req.newBuilder()
                .setDatabase(database)
                .setFilename(file.toAbsolutePath().toString())
                .build();
        ResponseObserver.Export streamObserver = new ResponseObserver.Export(new ProgressPrinter.Export());
        stub.exportData(req, streamObserver);
        streamObserver.await();
        return streamObserver.success();
    }

    static abstract class ResponseObserver<T> implements StreamObserver<T> {

        private final CountDownLatch latch;
        private boolean success;

        ResponseObserver() {
            this.latch = new CountDownLatch(1);
        }

        private static class Import extends ResponseObserver<MigratorProto.Import.Progress> {

            private final ProgressPrinter.Import progressPrinter;

            private Import(ProgressPrinter.Import progressPrinter) {
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

            private Export(ProgressPrinter.Export progressPrinter) {
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

        private final Timer timer = new Timer();
        private int anim = 0;
        private int linesPrinted = 0;
        Status status = Status.STARTING;

        private ProgressPrinter() {
            TimerTask task = new TimerTask() {
                @Override
                public void run() {
                    print();
                }
            };
            timer.scheduleAtFixedRate(task, 0, 100);
        }

        public void onCompleted() {
            status = Status.COMPLETED;
            timer.cancel();
            print();
        }

        abstract String type();

        abstract String formattedProgress();

        enum Status {
            STARTING("starting"),
            IN_PROGRESS("in progress"),
            COMPLETED("completed");

            private final String description;

            Status(String description) {
                this.description = description;
            }
        }

        private synchronized void print() {
            StringBuilder builder = new StringBuilder();
            builder.append(String.format("$x isa %s,\n    has status \"%s\";", type(), status.description));

            if (!status.equals(Status.STARTING)) {
                builder.append("\n\n");
                builder.append(formattedProgress());
                builder.append(";");
                anim = (anim + 1) % ANIM.length;
                builder.append(" ").append(ANIM[anim]);
            }

            String output = builder.toString();
            System.out.println((linesPrinted > 0 ? "\033[" + linesPrinted + "F\033[J" : "") + output);
            linesPrinted = output.split("\n").length;
        }

        String attributeProgress(long current, long attributes) {
            return attributes == 0 ? String.format("Attribute: %d", current) :
                    String.format("Attribute: %d/%d (%.1f%%)", current, attributes, 100.0 * current / attributes);
        }

        String entityProgress(long current, long entities) {
            return entities == 0 ? String.format("Entity: %d", current) :
                    String.format("Entity: %d/%d (%.1f%%)", current, entities, 100.0 * current / entities);
        }

        String relationProgress(long current, long relations) {
            return relations == 0 ? String.format("Relation: %d", current) :
                    String.format("Relation: %d/%d (%.1f%%)", current, relations, 100.0 * current / relations);
        }

        private static class Import extends ProgressPrinter {

            private MigratorProto.Import.Progress prog;

            void onProgress(MigratorProto.Import.Progress progress) {
                this.status = Status.IN_PROGRESS;
                this.prog = progress;
            }

            @Override
            String formattedProgress() {
                StringBuilder progressStr = new StringBuilder();
                progressStr.append(attributeProgress(prog.getAttributesCurrent(), prog.getAttributes()));
                long currentThings = prog.getAttributesCurrent();
                long things = prog.getAttributes();
                if (!prog.getInitialising()) {
                    progressStr.append(" ");
                    progressStr.append(prog.getOwnerships() == 0 ?
                            String.format("[ ownership: %d/%d ]", prog.getOwnershipsCurrent(), prog.getOwnerships()) :
                            String.format("[ ownership: %d/%d (%.1f%%) ]", prog.getOwnershipsCurrent(),
                                    prog.getOwnerships(), 100.0 * prog.getOwnershipsCurrent() / prog.getOwnerships()));
                    progressStr.append("\n");
                    progressStr.append(entityProgress(prog.getEntitiesCurrent(), prog.getEntities()));
                    progressStr.append("\n");
                    progressStr.append(relationProgress(prog.getRelationsCurrent(), prog.getRelations()));
                    progressStr.append(" ");
                    progressStr.append(prog.getRoles() == 0 ?
                            String.format("[ role: %d/%d ]", prog.getRolesCurrent(), prog.getRoles()) :
                            String.format("[ role: %d/%d (%.1f%%) ]", prog.getRolesCurrent(), prog.getRoles(),
                                    100.0 * prog.getRolesCurrent() / prog.getRoles()));
                    currentThings += prog.getEntitiesCurrent() + prog.getRelationsCurrent() + prog.getOwnershipsCurrent() +
                            prog.getRolesCurrent();
                    things += prog.getEntities() + prog.getRelations() + prog.getOwnerships() + prog.getRoles();

                    progressStr.append("\n");
                    progressStr.append(String.format("Total: %d/%d (%.1f%%)", currentThings, things, 100.0 * currentThings / things));
                }
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
                this.status = Status.IN_PROGRESS;
                this.prog = progress;
            }

            @Override
            String formattedProgress() {
                StringBuilder progressStr = new StringBuilder();
                progressStr.append(attributeProgress(prog.getAttributesCurrent(), prog.getAttributes()));
                progressStr.append("\n");
                progressStr.append(entityProgress(prog.getEntitiesCurrent(), prog.getEntities()));
                progressStr.append("\n");
                progressStr.append(relationProgress(prog.getRelationsCurrent(), prog.getRelations()));
                progressStr.append("\n");
                long currentThings = prog.getAttributesCurrent() + prog.getEntitiesCurrent() + prog.getRelationsCurrent();
                long things = prog.getAttributes() + prog.getEntities() + prog.getRelations();
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
