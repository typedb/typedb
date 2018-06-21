package pick;

import ai.grakn.GraknTx;
import strategy.PickableCollection;

import java.util.stream.Stream;

public class PickableCollectionValuePicker<T> implements StreamInterface<T> {

    private PickableCollection<T> valueOptions;

    public PickableCollectionValuePicker(PickableCollection<T> valueOptions) {
        this.valueOptions = valueOptions;
    }

    @Override
    public Stream<T> getStream(int streamLength, GraknTx tx) {
        return Stream.generate(() -> valueOptions.next()).limit(streamLength);
    }
}