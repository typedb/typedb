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

import grakn.core.common.parameters.Options;

import java.nio.file.Path;

public class RocksFactory implements Factory {

    private DatabaseManager<RocksGrakn> databaseManager;
    private Database<RocksGrakn> database;
    private Session<RocksDatabase> session;
    private TransactionSchema<RocksSession.Schema> transactionSchema;
    private TransactionData<RocksSession.Data> transactionData;
    private Storage<RocksDatabase, RocksTransaction> storage;
    private StorageReadOnly<RocksDatabase> storageReadOnly;

    @Override
    public RocksGrakn grakn(Path directory, Options.Database options) {
        return new RocksGrakn(directory, options, databaseManager());
    }

    private DatabaseManager<RocksGrakn> databaseManager() {
        if (databaseManager == null) {
            databaseManager = rocksGrakn -> new RocksDatabaseManager(rocksGrakn, database());
        }
        return databaseManager;
    }

    private Database<RocksGrakn> database() {
        if (database == null) {
            database = (rocksGrakn, name) -> new RocksDatabase(rocksGrakn, name, session());
        }
        return database;
    }

    private Session<RocksDatabase> session() {
        if (session == null) {
            session = new Session<RocksDatabase>() {

                @Override
                public RocksSession.Schema sessionSchema(RocksDatabase database, Options.Session options) {
                    return new RocksSession.Schema(database, options, transactionSchema());
                }

                @Override
                public RocksSession.Data sessionData(RocksDatabase database, Options.Session options) {
                    return new RocksSession.Data(database, options, transactionData());
                }

                @Override
                public StorageReadOnly<RocksDatabase> storageReadOnlyFactory() {
                    return storageReadOnly();
                }
            };
        }
        return session;
    }

    private TransactionSchema<RocksSession.Schema> transactionSchema() {
        if (transactionSchema == null) {
            transactionSchema = (session, type, options) -> new RocksTransaction.Schema(session, type, options, storage());
        }
        return transactionSchema;
    }

    private TransactionData<RocksSession.Data> transactionData() {
        if (transactionData == null) {
            transactionData = (session, type, options) -> new RocksTransaction.Data(session, type, options, storage());
        }
        return transactionData;
    }

    private Storage<RocksDatabase, RocksTransaction> storage() {
        if (storage == null) {
            storage = new Storage<RocksDatabase, RocksTransaction>() {
                @Override
                public RocksStorage.Schema storageSchema(RocksDatabase database, RocksTransaction transaction) {
                    return new RocksStorage.Schema(database, transaction);
                }

                @Override
                public RocksStorage.Data storageData(RocksDatabase database, RocksTransaction transaction) {
                    return new RocksStorage.Data(database, transaction);
                }
            };
        }
        return storage;
    }

    private StorageReadOnly<RocksDatabase> storageReadOnly() {
        if (storageReadOnly == null) {
            storageReadOnly = database -> new RocksStorage(database.rocksSchema(), true);
        }
        return storageReadOnly;
    }
}
