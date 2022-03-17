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

public interface Factory {

    CoreDatabaseManager databaseManager(Options.Database options);

    interface Database {

        CoreDatabase databaseCreateAndOpen(CoreDatabaseManager databaseMgr, String name);

        CoreDatabase databaseLoadAndOpen(CoreDatabaseManager databaseMgr, String name);
    }

    interface Session {

        CoreSession.Schema sessionSchema(CoreDatabase database, Options.Session options);

        CoreSession.Data sessionData(CoreDatabase database, Options.Session options);
    }

    interface TransactionSchema {

        CoreTransaction.Schema transaction(CoreSession.Schema session, Arguments.Transaction.Type type,
                                           Options.Transaction options);

        CoreTransaction.Schema initialisationTransaction(CoreSession.Schema session);
    }

    interface TransactionData {

        CoreTransaction.Data transaction(CoreSession.Data session, Arguments.Transaction.Type type,
                                         Options.Transaction options);
    }

    interface Storage {

        RocksStorage.Schema storageSchema(CoreDatabase database, CoreTransaction.Schema transaction);

        RocksStorage.Data storageData(CoreDatabase database, CoreTransaction transaction);
    }
}
