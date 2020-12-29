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

public class RocksFactory implements Factory {

    @Override
    public RocksGrakn grakn(Path directory, Options.Database options) {
        return new RocksGrakn(directory, options, this);
    }

    @Override
    public RocksDatabase database(RocksGrakn grakn, String name) {
        return RocksDatabase.create(grakn, name, this);
    }

    @Override
    public RocksSession.Schema sessionSchema(RocksDatabase database, Options.Session options) {
        return RocksSession.Schema.create(database, options, this);
    }

    @Override
    public RocksSession.Data sessionData(RocksDatabase database, Options.Session options) {
        return RocksSession.Data.create(database, options, this);
    }

    @Override
    public RocksTransaction.Schema transactionSchema(
            RocksSession.Schema session, Arguments.Transaction.Type type, Options.Transaction options) {
        return RocksTransaction.Schema.create(session, type, options, this);
    }

    @Override
    public RocksTransaction.Data transactionData(
            RocksSession.Data session, Arguments.Transaction.Type type, Options.Transaction options) {
        return RocksTransaction.Data.create(session, type, options, this);
    }

    @Override
    public RocksStorage.Schema storageSchema(RocksDatabase database, RocksTransaction.Schema transaction) {
        return RocksStorage.Schema.create(database, transaction);
    }

    @Override
    public RocksStorage.Data storageData(RocksDatabase database, RocksTransaction.Schema transaction) {
        return RocksStorage.Data.create(database, transaction);
    }
}
