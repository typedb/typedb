package pick;

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;

import java.util.stream.Stream;

public interface ConceptIdStreamInterface {
    Stream<ConceptId> getConceptIdStream(int numConceptIds, GraknTx tx);
}
