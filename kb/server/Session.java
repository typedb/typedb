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

package grakn.core.kb.server;

import grakn.core.kb.keyspace.AttributeManager;
import grakn.core.kb.keyspace.KeyspaceStatistics;
import grakn.core.kb.keyspace.ShardManager;
import grakn.core.kb.server.keyspace.Keyspace;

import java.util.function.Consumer;

public interface Session extends AutoCloseable {
    Transaction readTransaction();
    Transaction writeTransaction();

    /**
     * Method used by SessionFactory to register a callback function that has to be triggered when closing current session.
     *
     * @param onClose callback function (this should be used to update the session references in SessionFactory)
     */
    // NOTE: this method is used by Grakn KGMS and should be kept public
    void setOnClose(Consumer<Session> onClose);

    /**
     * Close JanusGraph, it will not be possible to create new transactions using current instance of Session.
     * This closes current session and local transaction, invoking callback function if one is set.
     **/
    void close();

    void invalidate();

    Keyspace keyspace();

    KeyspaceStatistics keyspaceStatistics();

    AttributeManager attributeManager();

    ShardManager shardManager();
}
