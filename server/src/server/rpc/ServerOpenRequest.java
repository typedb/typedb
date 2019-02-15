/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

import grakn.core.protocol.SessionProto;
import grakn.core.server.Transaction;
import grakn.core.server.keyspace.Keyspace;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.SessionStore;

/**
 * A request transaction opener for RPC Services. It requires the keyspace and transaction type from the argument object
 * to open a new transaction.
 */
public class ServerOpenRequest implements OpenRequest {

    private final SessionStore sessionStore;

    public ServerOpenRequest(SessionStore sessionStore) {
        this.sessionStore = sessionStore;
    }

    @Override
    public SessionImpl open(SessionProto.OpenSessionReq request) {
        Keyspace keyspace = Keyspace.of(request.getKeyspace());
        return sessionStore.session(keyspace);
    }

}
