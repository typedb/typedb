package pick;

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import pdf.PDF;

import java.util.stream.Stream;

public class ConceptIdStreamLimiter implements ConceptIdStreamLimiterInterface {
    ConceptIdStreamInterface conceptIdStreamer;

    public ConceptIdStreamLimiter (ConceptIdStreamInterface conceptIdStreamer) {
        this.conceptIdStreamer = conceptIdStreamer;
    }

    @Override
    public void reset() {
    }

    @Override
    public Stream<ConceptId> getConceptIdStream(PDF pdf, GraknTx tx) {
        // Simply limit the stream of ConceptIds to the number given by the pdf
        int numConceptIds = pdf.next();

        Stream<ConceptId> stream = this.conceptIdStreamer.getConceptIdStream(numConceptIds,tx);

        //TODO also check the stream in case it curtails with nulls?

        // Return the unadjusted stream but with a limit
        return stream.limit(numConceptIds);
    }
}

