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
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Options;
import grakn.protocol.SessionProto;
import grakn.protocol.TransactionProto;

import java.util.HashSet;
import java.util.Set;

import static com.google.protobuf.ByteString.copyFrom;
import static grakn.core.common.collection.Bytes.uuidToBytes;
import static grakn.core.server.rpc.util.RequestReader.getOptions;

class SessionRPC {

    private final Grakn.Session session;
    // TODO: does this need to be concurrent?
    private final Set<TransactionRPC> transactionRPCs;

    SessionRPC(Grakn grakn, SessionProto.Session.Open.Req request) {
        final Arguments.Session.Type sessionType = Arguments.Session.Type.of(request.getType().getNumber());
        final Options.Session options = getOptions(Options.Session::new, request.getOptions());
        session = grakn.session(request.getDatabase(), sessionType, options);
        transactionRPCs = new HashSet<>();
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
        transactionRPCs.parallelStream().forEach(TransactionRPC::close);
        session.close();
    }

    void closeWithError(Throwable error) {
        transactionRPCs.parallelStream().forEach(ts -> ts.closeWithError(error));
        session.close();
    }
}
