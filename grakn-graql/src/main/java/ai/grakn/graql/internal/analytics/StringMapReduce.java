package ai.grakn.graql.internal.analytics;

import ai.grakn.concept.TypeName;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 *
 */
public abstract class StringMapReduce extends GraknMapReduce<Set<String>> {

    public StringMapReduce() {
    }

    public StringMapReduce(Set<TypeName> selectedTypes) {
        super(selectedTypes);
    }

    @Override
    public void reduce(final Serializable key, final Iterator<Set<String>> values,
                       final ReduceEmitter<Serializable, Set<String>> emitter) {
        Set<String> set = new HashSet<>();
        while (values.hasNext()) {
            set.addAll(values.next());
        }
        emitter.emit(key, set);
    }


    @Override
    public void combine(final Serializable key, final Iterator<Set<String>> values,
                        final ReduceEmitter<Serializable, Set<String>> emitter) {
        this.reduce(key, values, emitter);
    }
}
