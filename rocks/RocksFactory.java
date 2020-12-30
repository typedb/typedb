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

package grakn.core.rocks;

import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Options;

import java.nio.file.Path;

public final class RocksFactory implements Factory {

    private Database databaseFactory;
    private Session sessionFactory;
    private TransactionSchema transactionSchemaFactory;
    private TransactionData transactionDataFactory;
    private Storage storageFactory;

    @Override
    public RocksGrakn grakn(Path directory, Options.Database options) {
        return new RocksGrakn(directory, options, databaseFactory());
    }

    private synchronized Factory.Database databaseFactory() {
        if (databaseFactory == null) {
            databaseFactory = new Database() {
                @Override
                public RocksDatabase databaseCreate(RocksGrakn grakn, String name) {
                    return RocksDatabase.createNewAndOpen(grakn, name, sessionFactory());
                }

                @Override
                public RocksDatabase databaseLoad(RocksGrakn grakn, String name) {
                    return RocksDatabase.loadExistingAndOpen(grakn, name, sessionFactory());
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
            transactionSchemaFactory = (session, type, options) ->
                    new RocksTransaction.Schema(session, type, options, storageFactory());
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
