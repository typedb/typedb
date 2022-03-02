/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.query.QueryManager;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

public interface TypeDB {

    interface DatabaseManager extends AutoCloseable {

        boolean isOpen();

        boolean contains(String name);

        Database create(String name);

        Database get(String name);

        Set<? extends Database> all();

        Session session(String database, Arguments.Session.Type type);

        Session session(String database, Arguments.Session.Type type, Options.Session options);

        void close();

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

        void cleanUp();

        FunctionalIterator<Pair<ByteArray, ByteArray>> committedIIDs();
    }
}
