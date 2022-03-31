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

public final class CoreFactory implements Factory {

    private Database databaseFactory;
    private Session sessionFactory;
    private TransactionSchema transactionSchemaFactory;
    private TransactionData transactionDataFactory;
    private Storage storageFactory;

    @Override
    public CoreDatabaseManager databaseManager(Options.Database options) {
        return new CoreDatabaseManager(options, databaseFactory());
    }

    private synchronized Factory.Database databaseFactory() {
        if (databaseFactory == null) {
            databaseFactory = new Database() {
                @Override
                public CoreDatabase databaseCreateAndOpen(CoreDatabaseManager databaseMgr, String name) {
                    return CoreDatabase.createAndOpen(databaseMgr, name, sessionFactory());
                }

                @Override
                public CoreDatabase databaseLoadAndOpen(CoreDatabaseManager databaseMgr, String name) {
                    return CoreDatabase.loadAndOpen(databaseMgr, name, sessionFactory());
                }
            };
        }
        return databaseFactory;
    }

    private synchronized Factory.Session sessionFactory() {
        if (sessionFactory == null) {
            sessionFactory = new Session() {

                @Override
                public CoreSession.Schema sessionSchema(CoreDatabase database, Options.Session options) {
                    return new CoreSession.Schema(database, Arguments.Session.Type.SCHEMA, options, transactionSchemaFactory());
                }

                @Override
                public CoreSession.Data sessionData(CoreDatabase database, Options.Session options) {
                    return new CoreSession.Data(database, Arguments.Session.Type.DATA, options, transactionDataFactory());
                }
            };
        }
        return sessionFactory;
    }

    private synchronized Factory.TransactionSchema transactionSchemaFactory() {
        if (transactionSchemaFactory == null) {
            transactionSchemaFactory = new Factory.TransactionSchema() {
                @Override
                public CoreTransaction.Schema transaction(CoreSession.Schema session, Arguments.Transaction.Type type, Options.Transaction options) {
                    return new CoreTransaction.Schema(session, type, options, storageFactory());
                }

                @Override
                public CoreTransaction.Schema transactionLocal(CoreSession.Schema session) {
                    return new CoreTransaction.Schema(session, Arguments.Transaction.Type.WRITE, new Options.Transaction(), storageFactory());
                }
            };
        }
        return transactionSchemaFactory;
    }

    private synchronized Factory.TransactionData transactionDataFactory() {
        if (transactionDataFactory == null) {
            transactionDataFactory = (session, type, options) ->
                    new CoreTransaction.Data(session, type, options, storageFactory());
        }
        return transactionDataFactory;
    }

    private synchronized Factory.Storage storageFactory() {
        if (storageFactory == null) {
            storageFactory = new Storage() {

                @Override
                public RocksStorage.Schema storageSchema(CoreDatabase database, CoreTransaction.Schema transaction) {
                    return new RocksStorage.Schema(database, transaction);
                }

                @Override
                public RocksStorage.Data storageData(CoreDatabase database, CoreTransaction transaction) {
                    return new RocksStorage.Data(database, transaction);
                }
            };
        }
        return storageFactory;
    }
}
