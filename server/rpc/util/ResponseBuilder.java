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

package grakn.core.server.rpc.util;

import grakn.core.common.exception.GraknException;
import grakn.protocol.TransactionProto;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * A utility class to build RPC Responses from a provided set of Grakn concepts.
 */
public class ResponseBuilder {

    public static StatusRuntimeException exception(Throwable e) {
        if (e instanceof GraknException) {
            return exception(Status.INTERNAL, e.getMessage());
        } else if (e instanceof StatusRuntimeException) {
            return (StatusRuntimeException) e;
        } else {
            return exception(Status.UNKNOWN, e.getMessage());
        }
    }

    private static StatusRuntimeException exception(Status status, String message) {
        return status.withDescription(message + " Please check server logs for the stack trace.").asRuntimeException();
    }

    /**
     * An RPC Response Builder class for Transaction responses
     */
    public static class Transaction {

        public static TransactionProto.Transaction.Res open() {
            return TransactionProto.Transaction.Res.newBuilder()
                    .setOpenRes(TransactionProto.Transaction.Open.Res.getDefaultInstance())
                    .build();
        }

        static TransactionProto.Transaction.Res commit() {
            return TransactionProto.Transaction.Res.newBuilder()
                    .setCommitRes(TransactionProto.Transaction.Commit.Res.getDefaultInstance())
                    .build();
        }

        /**
         * An RPC Response Builder class for Transaction iterator responses
         */
        public static class Iter {

            public static TransactionProto.Transaction.Res done() {
                return TransactionProto.Transaction.Res.newBuilder()
                        .setIterRes(TransactionProto.Transaction.Iter.Res.newBuilder()
                                            .setDone(true)).build();
            }

            public static TransactionProto.Transaction.Res id(int id) {
                return TransactionProto.Transaction.Res.newBuilder()
                        .setIterRes(TransactionProto.Transaction.Iter.Res.newBuilder()
                                            .setIteratorId(id)).build();
            }
        }
    }
}
