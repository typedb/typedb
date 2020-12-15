package grakn.core.server.migrator;

import grakn.core.server.migrator.proto.MigratorProto;

public interface Migrator {

    void run();
    MigratorProto.Job.Progress getProgress();
}
