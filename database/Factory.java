/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
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
