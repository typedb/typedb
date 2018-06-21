package pick;

import ai.grakn.GraknTx;
import pdf.PDF;

import java.util.stream.Stream;

public interface StreamProviderInterface<T> {
        Stream<T> getStream(PDF pdf, GraknTx tx);  // TODO Change from pdf to streamLength for the benefit of AttributeGenerator

        void reset();
}
