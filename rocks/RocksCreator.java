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

    public RocksStorage.Schema schemaStorage(RocksDatabase database, RocksTransaction transaction) {
        return new RocksStorage.Schema(database, transaction);
    }

    public RocksStorage.Data dataStorage(RocksDatabase database, RocksTransaction transaction) {
        return new RocksStorage.Data(database, transaction);
    }

    public RocksStorage rocksStorage(RocksDatabase database) {
        return new RocksStorage(database.rocksSchema(), true);
    }

    public RocksTransaction.Schema transactionSchema(RocksSession.Schema session, Arguments.Transaction.Type type, Options.Transaction options) {
        return new RocksTransaction.Schema(session, type, options, this);
    }

    public RocksTransaction.Data transactionData(RocksSession.Data session, Arguments.Transaction.Type type, Options.Transaction options) {
        return new RocksTransaction.Data(session, type, options, this);
    }
}
