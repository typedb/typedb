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

package grakn.core;

import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Options;
import grakn.core.concept.ConceptManager;
import grakn.core.logic.LogicManager;
import grakn.core.query.QueryManager;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * A Grakn Database API
 *
 * This interface allows you to open a 'Session' to connect to the Grakn
 * database, and open a 'Transaction' from that session.
 */
public interface Grakn extends AutoCloseable {

    Session session(String database, Arguments.Session.Type type);

    Session session(String database, Arguments.Session.Type type, Options.Session options);

    DatabaseManager databases();

    boolean isOpen();

    void close();

    /**
     * Grakn Database Manager
     */
    interface DatabaseManager {

        boolean contains(String name);

        Database create(String name);

        Database get(String name);

        Set<? extends Database> all();
    }

    /**
     * A Grakn Database
     *
     * A database is an isolated scope of data in the storage engine.
     */
    interface Database {

        String name();

        boolean contains(UUID sessionID);

        Session get(UUID sessionID);

        Stream<Session> sessions();

        void delete();
    }

    /**
     * A Grakn Database Session
     *
     * This interface allows you to create transactions to perform READs or
     * WRITEs onto the database.
     */
    interface Session extends AutoCloseable {

        Transaction transaction(Arguments.Transaction.Type type);

        Transaction transaction(Arguments.Transaction.Type type, Options.Transaction options);

        UUID uuid();

        Arguments.Session.Type type();

        Database database();

        boolean isOpen();

        void close();
    }

    /**
     * A Grakn Database Transaction
     */
    interface Transaction extends AutoCloseable {

        Arguments.Transaction.Type type();

        Options.Transaction options();

        boolean isOpen();

        ConceptManager concepts();

        LogicManager logics();

        QueryManager query();

        void commit();

        void rollback();

        void close();
    }
}
