package ai.grakn.engine;

import ai.grakn.GraknConfigKey;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.factory.SystemKeyspaceSession;
import ai.grakn.kb.internal.EmbeddedGraknTx;

public class SystemKeyspaceSessionProvider implements SystemKeyspaceSession {

    //TODO: centralise this
    private final static Keyspace SYSTEM_KB_KEYSPACE = Keyspace.of("graknsystem");
    private final EmbeddedGraknSession session;

    public SystemKeyspaceSessionProvider(GraknConfig config) {
        session = EmbeddedGraknSession.createEngineSession(SYSTEM_KB_KEYSPACE, engineURI(config), config);
    }

    @Override
    public EmbeddedGraknTx tx(GraknTxType txType) {
        return session.open(txType);
    }

    private String engineURI(GraknConfig config) {
        return config.getProperty(GraknConfigKey.SERVER_HOST_NAME) + ":" + config.getProperty(GraknConfigKey.SERVER_PORT);
    }
}
