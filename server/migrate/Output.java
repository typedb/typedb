package grakn.core.server.migrate;

import grakn.core.server.migrate.proto.DataProto;

public interface Output {
    void write(DataProto.Item item) throws Exception;
}
