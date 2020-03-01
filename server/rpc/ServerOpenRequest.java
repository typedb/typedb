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

import grakn.core.kb.server.Session;
import grakn.core.server.keyspace.KeyspaceImpl;
import grakn.core.server.session.SessionFactory;
import grakn.protocol.session.SessionProto;

/**
 * A request transaction opener for RPC Services. It requires the keyspace and transaction type from the argument object
 * to open a new transaction.
 */
public class ServerOpenRequest implements OpenRequest {

    private final SessionFactory sessionFactory;

    public ServerOpenRequest(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @Override
    public Session open(SessionProto.Session.Open.Req request) {
        KeyspaceImpl keyspace = new KeyspaceImpl(request.getKeyspace());
        return sessionFactory.session(keyspace);
    }

}
