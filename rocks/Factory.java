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

public interface Factory {

    RocksGrakn grakn(Path directory, Options.Database options);

    interface DatabaseManager<G extends RocksGrakn> {

        RocksDatabaseManager databaseManager(G rocksGrakn);
    }

    interface Database<G extends RocksGrakn> {

        RocksDatabase database(G rocksGrakn, String name);
    }

    interface Session<D extends RocksDatabase> {

        RocksSession.Schema sessionSchema(D database, Options.Session options);

        RocksSession.Data sessionData(D database, Options.Session options);

        StorageReadOnly<D> storageReadOnlyFactory();
    }

    interface TransactionSchema<S extends RocksSession.Schema> {

        RocksTransaction.Schema transactionSchema(S session, Arguments.Transaction.Type type, Options.Transaction options);
    }

    interface TransactionData<S extends RocksSession.Data> {

        RocksTransaction.Data transactionData(S session, Arguments.Transaction.Type type, Options.Transaction options);
    }

    interface Storage<D extends RocksDatabase, T extends RocksTransaction> {

        RocksStorage.Schema storageSchema(D database, T transaction);

        RocksStorage.Data storageData(D database, T transaction);
    }

    interface StorageReadOnly<D extends RocksDatabase> {

        RocksStorage storageSchemaReadOnly(D database);
    }
}
