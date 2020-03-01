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

import grakn.protocol.keyspace.KeyspaceProto;


/**
 * This interface has 2 different implementations in Grakn Core and Grakn KGMS
 * It is used to handle retrieve and delete keyspace requests.
 */
public interface KeyspaceRequestsHandler {

    Iterable<String> retrieve(KeyspaceProto.Keyspace.Retrieve.Req request);

    void delete(KeyspaceProto.Keyspace.Delete.Req request);

}
