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
        boolean[] result = new boolean[1];
        ProgressPrinter progressPrinter = new ProgressPrinter("import");
        stub.importData(req, new StreamObserver<MigratorProto.Job.Res>() {
            @Override
            public void onNext(MigratorProto.Job.Res res) {
                long current = res.getProgress().getCurrent();
                long total = res.getProgress().getTotal();
                progressPrinter.onProgress(current, total);
            }

            @Override
            public void onError(Throwable throwable) {
                throwable.printStackTrace();
                result[0] = false;
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                progressPrinter.onCompletion();
                result[0] = true;
                latch.countDown();
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw GraknException.of(ErrorMessage.Internal.UNEXPECTED_INTERRUPTION);
        }
        return result[0];
    }
}
