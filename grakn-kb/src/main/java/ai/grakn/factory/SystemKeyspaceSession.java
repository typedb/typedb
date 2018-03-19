package ai.grakn.factory;

import ai.grakn.GraknTxType;
import ai.grakn.kb.internal.EmbeddedGraknTx;

public interface SystemKeyspaceSession {
    EmbeddedGraknTx tx(GraknTxType txType);
}
