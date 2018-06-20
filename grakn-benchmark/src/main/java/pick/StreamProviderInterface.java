package pick;

import ai.grakn.GraknTx;
import pdf.PDF;

import java.util.stream.Stream;

public interface StreamProviderInterface<T> {
        Stream<T> getStream(PDF pdf, GraknTx tx);

        void reset();
}
