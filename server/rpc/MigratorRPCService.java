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

package grakn.core.server.rpc;

import grakn.core.Grakn;
import grakn.core.server.migrator.proto.MigratorGrpc;
import grakn.core.server.migrator.proto.MigratorProto;
import io.grpc.stub.StreamObserver;

public class MigratorRPCService extends MigratorGrpc.MigratorImplBase {

    private final Grakn grakn;

    public MigratorRPCService(Grakn grakn) {
        this.grakn = grakn;
    }

    @Override
    public void exportData(MigratorProto.ExportData.Req request, StreamObserver<MigratorProto.Job.Res> responseObserver) {
    }

    @Override
    public void importData(MigratorProto.ImportData.Req request, StreamObserver<MigratorProto.Job.Res> responseObserver) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < 100; i++) {
            MigratorProto.Job.Res res = MigratorProto.Job.Res.newBuilder().setProgress(
                    MigratorProto.Job.Progress.newBuilder().setCurrent(i + 1).setTotal(100).build()
            ).build();
            responseObserver.onNext(res);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        responseObserver.onCompleted();
    }

    @Override
    public void exportSchema(MigratorProto.ExportSchema.Req request, StreamObserver<MigratorProto.ExportSchema.Res> responseObserver) {
    }
}
