package grakn.core.server.rpc;

import grakn.protocol.keyspace.KeyspaceProto;

public interface KeyspaceRequestsHandler {
    Iterable<String> retrieve(KeyspaceProto.Keyspace.Retrieve.Req request);
    void delete(KeyspaceProto.Keyspace.Delete.Req request);
}
