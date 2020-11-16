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

import com.google.protobuf.ByteString;
import grakn.core.Grakn;
import grakn.protocol.TransactionProto;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.protobuf.ByteString.copyFrom;
import static grakn.core.common.collection.Bytes.uuidToBytes;

class SessionRPC {

    private final Grakn.Session session;
    private final GraknRPCService graknRPCService;
    private final Set<TransactionRPC> transactionRPCs;
    private final AtomicBoolean isOpen;

    SessionRPC(GraknRPCService graknRPCService, Grakn.Session session) {
        this.graknRPCService = graknRPCService;
        this.session = session;
        transactionRPCs = new HashSet<>();
        isOpen = new AtomicBoolean(true);
    }

    TransactionRPC transaction(TransactionStream transactionStream, TransactionProto.Transaction.Open.Req request) {
        final TransactionRPC transactionRPC = new TransactionRPC(this, transactionStream, request);
        transactionRPCs.add(transactionRPC);
        return transactionRPC;
    }

    Grakn.Session session() {
        return session;
    }

    ByteString uuidAsByteString() {
        return copyFrom(uuidToBytes(session.uuid()));
    }

    void remove(TransactionRPC transactionRPC) {
        transactionRPCs.remove(transactionRPC);
    }

    void close() {
        if (isOpen.compareAndSet(true, false)) {
            final Set<TransactionRPC> transactionRPCsCopy = new HashSet<>(transactionRPCs);
            transactionRPCsCopy.parallelStream().forEach(TransactionRPC::close);
            session.close();
            graknRPCService.removeSession(session.uuid());
        }
    }

    void closeWithError(Throwable error) {
        if (isOpen.compareAndSet(true, false)) {
            final Set<TransactionRPC> transactionRPCsCopy = new HashSet<>(transactionRPCs);
            transactionRPCsCopy.parallelStream().forEach(tr -> tr.closeWithError(error));
            session.close();
            graknRPCService.removeSession(session.uuid());
        }
    }
}
