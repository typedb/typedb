package ai.grakn.engine.util;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.rpc.util.TxConceptReader;
import ai.grakn.rpc.generated.GrpcConcept;

public class EmbeddedConceptReader extends TxConceptReader {

    private EmbeddedGraknTx tx;
    public EmbeddedConceptReader(EmbeddedGraknTx tx) {
        this.tx = tx;
    }

    @Override
    public Concept concept(GrpcConcept.Concept grpcConcept) {
        return tx.getConcept(ConceptId.of(grpcConcept.getId().getValue()));
    }
}
