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

public class RocksCreator {
    public RocksGrakn grakn(Path directory, Options.Database options) {
        return new RocksGrakn(directory, options, this);
    }

    public RocksDatabaseManager databaseManager(RocksGrakn rocksGrakn) {
        return new RocksDatabaseManager(rocksGrakn, this);
    }

    public RocksDatabase database(RocksGrakn rocksGrakn, String name, boolean isNew) {
        return new RocksDatabase(rocksGrakn, name, isNew, this);
    }

    public RocksSession.Schema sessionSchema(RocksDatabase database, Options.Session options) {
        return new RocksSession.Schema(database, options, this);
    }

    public RocksSession.Data sessionData(RocksDatabase database, Options.Session options) {
        return new RocksSession.Data(database, options, this);
    }

    public RocksStorage.Schema storageSchema(RocksDatabase database, RocksTransaction transaction) {
        return new RocksStorage.Schema(database, transaction);
    }

    public RocksStorage storageSchemaReadOnly(RocksDatabase database) {
        return new RocksStorage(database.rocksSchema(), true);
    }

    public RocksStorage.Data storageData(RocksDatabase database, RocksTransaction transaction) {
        return new RocksStorage.Data(database, transaction);
    }

    public RocksTransaction.Schema transactionSchema(RocksSession.Schema session, Arguments.Transaction.Type type, Options.Transaction options) {
        return new RocksTransaction.Schema(session, type, options, this);
    }

    public RocksTransaction.Data transactionData(RocksSession.Data session, Arguments.Transaction.Type type, Options.Transaction options) {
        return new RocksTransaction.Data(session, type, options, this);
    }
}
