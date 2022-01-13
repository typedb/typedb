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

package com.vaticle.typedb.core.database;

import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;

public final class FactoryImpl implements Factory {
    private Database databaseFactory;
    private Session sessionFactory;
    private TransactionSchema transactionSchemaFactory;
    private TransactionData transactionDataFactory;
    private Storage storageFactory;

    @Override
    public DatabaseManagerImpl databaseManager(Options.Database options) {
        return new DatabaseManagerImpl(options, databaseFactory());
    }

    private synchronized Factory.Database databaseFactory() {
        if (databaseFactory == null) {
            databaseFactory = new Database() {
                @Override
                public DatabaseImpl databaseCreateAndOpen(DatabaseManagerImpl databaseManager, String name) {
                    return DatabaseImpl.createAndOpen(databaseManager, name, sessionFactory());
                }

                @Override
                public DatabaseImpl databaseLoadAndOpen(DatabaseManagerImpl databaseManager, String name) {
                    return DatabaseImpl.loadAndOpen(databaseManager, name, sessionFactory());
                }
            };
        }
        return databaseFactory;
    }

    private synchronized Factory.Session sessionFactory() {
        if (sessionFactory == null) {
            sessionFactory = new Session() {

                @Override
                public SessionImpl.Schema sessionSchema(DatabaseImpl database, Options.Session options) {
                    return new SessionImpl.Schema(database, Arguments.Session.Type.SCHEMA, options, transactionSchemaFactory());
                }

                @Override
                public SessionImpl.Data sessionData(DatabaseImpl database, Options.Session options) {
                    return new SessionImpl.Data(database, Arguments.Session.Type.DATA, options, transactionDataFactory());
                }
            };
        }
        return sessionFactory;
    }

    private synchronized Factory.TransactionSchema transactionSchemaFactory() {
        if (transactionSchemaFactory == null) {
            transactionSchemaFactory = new Factory.TransactionSchema() {
                @Override
                public TransactionImpl.Schema transaction(SessionImpl.Schema session, Arguments.Transaction.Type type, Options.Transaction options) {
                    return new TransactionImpl.Schema(session, type, options, storageFactory());
                }

                @Override
                public TransactionImpl.Schema initialisationTransaction(SessionImpl.Schema session) {
                    return new TransactionImpl.Schema(session, Arguments.Transaction.Type.WRITE, new Options.Transaction(), storageFactory());
                }
            };
        }
        return transactionSchemaFactory;
    }

    private synchronized Factory.TransactionData transactionDataFactory() {
        if (transactionDataFactory == null) {
            transactionDataFactory = (session, type, options) ->
                    new TransactionImpl.Data(session, type, options, storageFactory());
        }
        return transactionDataFactory;
    }

    private synchronized Factory.Storage storageFactory() {
        if (storageFactory == null) {
            storageFactory = new Storage() {

                @Override
                public StorageImpl.Schema storageSchema(DatabaseImpl database, TransactionImpl.Schema transaction) {
                    return new StorageImpl.Schema(database, transaction);
                }

                @Override
                public StorageImpl.Data storageData(DatabaseImpl database, TransactionImpl transaction) {
                    return new StorageImpl.Data(database, transaction);
                }
            };
        }
        return storageFactory;
    }
}
