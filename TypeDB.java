/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
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

        boolean isEmpty();

        boolean contains(UUID sessionID);

        Session session(UUID sessionID);

        Stream<Session> sessions();

        String schema();

        String typeSchema();

        String ruleSchema();

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

        FunctionalIterator<Pair<ByteArray, ByteArray>> committedIIDs();
    }
}
