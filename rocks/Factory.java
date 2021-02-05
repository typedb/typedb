/*
 * Copyright (C) 2021 Grakn Labs
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

    interface DatabaseManager {

        RocksDatabaseManager databaseManager(RocksGrakn grakn);
    }

    interface Database {

        RocksDatabase databaseCreateAndOpen(RocksGrakn grakn, String name);

        RocksDatabase databaseLoadAndOpen(RocksGrakn grakn, String name);
    }

    interface Session {

        RocksSession.Schema sessionSchema(RocksDatabase database, Options.Session options);

        RocksSession.Data sessionData(RocksDatabase database, Options.Session options);
    }

    interface TransactionSchema {

        RocksTransaction.Schema transaction(RocksSession.Schema session, Arguments.Transaction.Type type,
                                            Options.Transaction options, boolean internal);
    }

    interface TransactionData {

        RocksTransaction.Data transaction(RocksSession.Data session, Arguments.Transaction.Type type,
                                          Options.Transaction options);
    }

    interface Storage {

        RocksStorage.Schema storageSchema(RocksDatabase database, RocksTransaction.Schema transaction);

        RocksStorage.Data storageData(RocksDatabase database, RocksTransaction transaction);
    }
}
