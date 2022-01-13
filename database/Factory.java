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

    DatabaseManagerImpl databaseManager(Options.Database options);

    interface Database {

        DatabaseImpl databaseCreateAndOpen(DatabaseManagerImpl databaseManager, String name);

        DatabaseImpl databaseLoadAndOpen(DatabaseManagerImpl databaseManager, String name);
    }

    interface Session {

        SessionImpl.Schema sessionSchema(DatabaseImpl database, Options.Session options);

        SessionImpl.Data sessionData(DatabaseImpl database, Options.Session options);
    }

    interface TransactionSchema {

        TransactionImpl.Schema transaction(SessionImpl.Schema session, Arguments.Transaction.Type type,
                                           Options.Transaction options);

        TransactionImpl.Schema initialisationTransaction(SessionImpl.Schema session);
    }

    interface TransactionData {

        TransactionImpl.Data transaction(SessionImpl.Data session, Arguments.Transaction.Type type,
                                         Options.Transaction options);
    }

    interface Storage {

        StorageImpl.Schema storageSchema(DatabaseImpl database, TransactionImpl.Schema transaction);

        StorageImpl.Data storageData(DatabaseImpl database, TransactionImpl transaction);
    }
}
