package pick;

import ai.grakn.GraknTx;
import pdf.PDF;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Stream;

public class CentralStreamProvider<T> implements StreamProviderInterface<T> {
    StreamInterface<T> streamer;
    private Boolean isReset;
    private ArrayList<T> conceptIdList;

    public CentralStreamProvider(StreamInterface<T> streamer) {
        this.streamer = streamer;
        this.isReset = true;
    }

    @Override
    public void reset() {
        this.isReset = true;
    }

    @Override
    public Stream<T> getStream(PDF pdf, GraknTx tx) {
        // Get the same list as used previously, or generate one if not seen before
        // Only create a new stream if reset() has been called prior

        int streamLength = pdf.next();
        if (this.isReset) {

            Stream<T> stream = this.streamer.getStream(streamLength,tx);
            //TODO read stream to list and store to be used again later

            this.conceptIdList = new ArrayList<>();


            Iterator<T> iter = stream.limit(streamLength).iterator();
            while (iter.hasNext()) {
                this.conceptIdList.add(iter.next());
            }

            this.isReset = false;
        }
        // Return the same stream as before
        return this.conceptIdList.stream();
    }
}

