/*
 * Copyright (C) 2021 Grakn Labs
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
import grakn.core.common.parameters.Context;
import grakn.core.common.parameters.Options;
import grakn.core.concept.ConceptManager;
import grakn.core.logic.LogicManager;
import grakn.core.query.QueryManager;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

public interface Grakn extends AutoCloseable {

    Session session(String database, Arguments.Session.Type type);

    Session session(String database, Arguments.Session.Type type, Options.Session options);

    DatabaseManager databases();

    boolean isOpen();

    void close();

    interface DatabaseManager {

        boolean contains(String name);

        Database create(String name);

        Database get(String name);

        Set<? extends Database> all();
    }

    interface Database {

        String name();

        boolean contains(UUID sessionID);

        Session session(UUID sessionID);

        Stream<Session> sessions();

        String schema();

        void delete();
    }

    interface Session extends AutoCloseable {

        Transaction transaction(Arguments.Transaction.Type type);

        Transaction transaction(Arguments.Transaction.Type type, Options.Transaction options);

        UUID uuid();

        Arguments.Session.Type type();

        Database database();

        boolean isOpen();

        void close();
    }

    interface Transaction extends AutoCloseable {

        Arguments.Transaction.Type type();

        Context.Transaction context();

        boolean isOpen();

        ConceptManager concepts();

        LogicManager logic();

        QueryManager query();

        void commit();

        void rollback();

        void close();
    }
}
