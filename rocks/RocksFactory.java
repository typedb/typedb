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

import grakn.core.common.parameters.Context;
import grakn.core.common.parameters.Options;
import grakn.core.graph.util.KeyGenerator;
import org.rocksdb.OptimisticTransactionDB;

import java.nio.file.Path;

public class RocksFactory implements Factory {

    private Database database;
    private Session session;
    private TransactionSchema transactionSchema;
    private TransactionData transactionData;
    private Storage storage;

    @Override
    public RocksGrakn grakn(Path directory, Options.Database options) {
        return new RocksGrakn(directory, options, databaseFactory());
    }

    private Factory.Database databaseFactory() {
        if (database == null) database = name -> new RocksDatabase(name, sessionFactory());
        return database;
    }

    private Factory.Session sessionFactory() {
        if (session == null) {
            session = new Session() {

                @Override
                public RocksSession.Schema sessionSchema(Context.Session context) {
                    return new RocksSession.Schema(context, transactionSchemaFactory());
                }

                @Override
                public RocksSession.Data sessionData(Context.Session context) {
                    return new RocksSession.Data(context, transactionDataFactory());
                }
            };
        }
        return session;
    }

    private Factory.TransactionSchema transactionSchemaFactory() {
        if (transactionSchema == null) {
            transactionSchema = (context) -> new RocksTransaction.Schema(context, storageFactory());
        }
        return transactionSchema;
    }

    private Factory.TransactionData transactionDataFactory() {
        if (transactionData == null) {
            transactionData = (context) -> new RocksTransaction.Data(context, storageFactory());
        }
        return transactionData;
    }

    private Factory.Storage storageFactory() {
        if (storage == null) {
            storage = new Storage() {
                @Override
                public RocksStorage.Schema storageSchema(OptimisticTransactionDB rocksDB, KeyGenerator.Schema keyGenerator, boolean isRead) {
                    return new RocksStorage.Schema(rocksDB, keyGenerator, isRead);
                }

                @Override
                public RocksStorage.Data storageData(OptimisticTransactionDB rocksDB, KeyGenerator.Data keyGenerator, boolean isRead) {
                    return new RocksStorage.Data(rocksDB, keyGenerator, isRead);
                }
            };
        }
        return storage;
    }
}
