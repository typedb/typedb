package pick;

import ai.grakn.GraknTx;
import pdf.PDF;

import java.util.stream.Stream;

public class StreamProvider<T> implements StreamProviderInterface<T> {
    private StreamInterface<T> streamer;

    public StreamProvider(StreamInterface<T> streamer) {
        this.streamer = streamer;
    }

    @Override
    public void reset() {
    }

    @Override
    public Stream<T> getStream(PDF pdf, GraknTx tx) {
        // Simply limit the stream of ConceptIds to the number given by the pdf
        int streamLength = pdf.next();

        Stream<T> stream = this.streamer.getStream(streamLength,tx);

        //TODO also check the stream in case it curtails with nulls?

        // Return the unadjusted stream but with a limit
        return stream.limit(streamLength);
    }
}

