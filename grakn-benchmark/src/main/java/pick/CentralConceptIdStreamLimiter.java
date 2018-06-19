package pick;

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import pdf.PDF;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Stream;

public class CentralConceptIdStreamLimiter implements ConceptIdStreamLimiterInterface {
    ConceptIdStreamInterface conceptIdStreamer;
    private Boolean isReset;
    private ArrayList<ConceptId> conceptIdList;

    public CentralConceptIdStreamLimiter(ConceptIdStreamInterface conceptIdStreamer) {
        this.conceptIdStreamer = conceptIdStreamer;
        this.isReset = true;
    }

    @Override
    public void reset() {
        this.isReset = true;
    }

    @Override
    public Stream<ConceptId> getConceptIdStream(PDF pdf, GraknTx tx) {
        // Get the same list as used previously, or generate one if not seen before
        // Only create a new stream if reset() has been called prior

        int numConceptIds = pdf.next();
        if (this.isReset) {

            Stream<ConceptId> stream = this.conceptIdStreamer.getConceptIdStream(numConceptIds,tx);
            //TODO read stream to list and store to be used again later

            this.conceptIdList = new ArrayList<>();


            Iterator<ConceptId> iter = stream.limit(numConceptIds).iterator();
//            for (int i = 0; i <= numConceptIds; i++) {
            while (iter.hasNext()) {
                this.conceptIdList.add(iter.next());
            }

            this.isReset = false;
        }
        // Return the same stream as before
        return this.conceptIdList.stream();
    }
}

