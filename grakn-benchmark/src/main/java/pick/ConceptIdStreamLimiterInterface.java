package pick;

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import pdf.PDF;

import java.util.stream.Stream;

public interface ConceptIdStreamLimiterInterface {
        Stream<ConceptId> getConceptIdStream(PDF pdf, GraknTx tx);

        void reset();
}
