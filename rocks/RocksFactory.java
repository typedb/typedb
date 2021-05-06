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

package com.vaticle.typedb.core.rocks;

import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;

public final class RocksFactory implements Factory {
    private DatabaseManager databaseManagerFactory;
    private Database databaseFactory;
    private Session sessionFactory;
    private TransactionSchema transactionSchemaFactory;
    private TransactionData transactionDataFactory;
    private Storage storageFactory;

    @Override
    public RocksTypeDB typedb(Options.Database options) {
        return new RocksTypeDB(options, databaseManagerFactory());
    }

    private synchronized DatabaseManager databaseManagerFactory() {
        if (databaseManagerFactory == null) {
            databaseManagerFactory = typedb -> new RocksDatabaseManager(typedb, databaseFactory());
        }
        return databaseManagerFactory;
    }

    private synchronized Factory.Database databaseFactory() {
        if (databaseFactory == null) {
            databaseFactory = new Database() {
                @Override
                public RocksDatabase databaseCreateAndOpen(RocksTypeDB typedb, String name) {
                    return RocksDatabase.createAndOpen(typedb, name, sessionFactory());
                }

                @Override
                public RocksDatabase databaseLoadAndOpen(RocksTypeDB typedb, String name) {
                    return RocksDatabase.loadAndOpen(typedb, name, sessionFactory());
                }
            };
        }
        return databaseFactory;
    }

    private synchronized Factory.Session sessionFactory() {
        if (sessionFactory == null) {
            sessionFactory = new Session() {

                @Override
                public RocksSession.Schema sessionSchema(RocksDatabase database, Options.Session options) {
                    return new RocksSession.Schema(database, Arguments.Session.Type.SCHEMA, options, transactionSchemaFactory());
                }

                @Override
                public RocksSession.Data sessionData(RocksDatabase database, Options.Session options) {
                    return new RocksSession.Data(database, Arguments.Session.Type.DATA, options, transactionDataFactory());
                }
            };
        }
        return sessionFactory;
    }

    private synchronized Factory.TransactionSchema transactionSchemaFactory() {
        if (transactionSchemaFactory == null) {
            transactionSchemaFactory = new Factory.TransactionSchema() {
                @Override
                public RocksTransaction.Schema transaction(RocksSession.Schema session, Arguments.Transaction.Type type, Options.Transaction options) {
                    return new RocksTransaction.Schema(session, type, options, storageFactory());
                }

                @Override
                public RocksTransaction.Schema initialisationTransaction(RocksSession.Schema session) {
                    return new RocksTransaction.Schema(session, Arguments.Transaction.Type.WRITE, new Options.Transaction(), storageFactory());
                }
            };
        }
        return transactionSchemaFactory;
    }

    private synchronized Factory.TransactionData transactionDataFactory() {
        if (transactionDataFactory == null) {
            transactionDataFactory = (session, type, options) ->
                    new RocksTransaction.Data(session, type, options, storageFactory());
        }
        return transactionDataFactory;
    }

    private synchronized Factory.Storage storageFactory() {
        if (storageFactory == null) {
            storageFactory = new Storage() {
                @Override
                public RocksStorage.Schema storageSchema(RocksDatabase database, RocksTransaction.Schema transaction) {
                    return new RocksStorage.Schema(database, transaction);
                }

                @Override
                public RocksStorage.Data storageData(RocksDatabase database, RocksTransaction transaction) {
                    return new RocksStorage.Data(database, transaction);
                }
            };
        }
        return storageFactory;
    }
}
