package pick;

import ai.grakn.GraknTx;
import java.util.stream.Stream;

public interface StreamInterface<T> {
    Stream<T> getStream(int streamLength, GraknTx tx);
}
