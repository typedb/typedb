package io.mindmaps.graql.internal.analytics;

import io.mindmaps.util.ErrorMessage;
import io.mindmaps.util.Schema;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.*;

import static io.mindmaps.graql.internal.analytics.Analytics.TYPE;
import static io.mindmaps.graql.internal.analytics.Analytics.getVertexType;

public class MaxMapReduce implements MapReduce<Serializable, Long, Serializable, Long, Map<Serializable, Long>> {

    public static final String MAX_MEMORY_KEY = "analytics.maxMapReduce.memoryKey";
    public static final String DEFAULT_MEMORY_KEY = "count";

    private String memoryKey = DEFAULT_MEMORY_KEY;

    private Set<String> selectedTypes = null;

    public MaxMapReduce() {
    }

    public MaxMapReduce(Set<String> types) {
        selectedTypes = types;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<Serializable, Long> emitter) {
        if (selectedTypes.contains(getVertexType(vertex))) {
            emitter.emit(this.memoryKey, vertex.value(Schema.ConceptProperty.VALUE_LONG.name()));
            return;
        }
        emitter.emit(this.memoryKey, Long.MIN_VALUE);
    }

    @Override
    public void reduce(final Serializable key, final Iterator<Long> values,
                       final ReduceEmitter<Serializable, Long> emitter) {
        emitter.emit(key, IteratorUtils.reduce(values, Long.MIN_VALUE, (a, b) -> a > b ? a : b));
    }

    @Override
    public void combine(final Serializable key, final Iterator<Long> values,
                        final ReduceEmitter<Serializable, Long> emitter) {
        this.reduce(key, values, emitter);
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(MAX_MEMORY_KEY, this.memoryKey);
        configuration.setProperty(MAP_REDUCE, CountMapReduce.class.getName());
        Iterator iterator = selectedTypes.iterator();
        int count = 0;
        while (iterator.hasNext()) {
            configuration.addProperty(TYPE + "." + count, iterator.next());
            count++;
        }
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        this.memoryKey = configuration.getString(MAX_MEMORY_KEY, DEFAULT_MEMORY_KEY);
        this.selectedTypes = new HashSet<>();
        configuration.getKeys(TYPE).forEachRemaining(key -> selectedTypes.add(configuration.getString(key)));
    }

    @Override
    public boolean doStage(Stage stage) {
        return true;
    }

    @Override
    public Map<Serializable, Long> generateFinalResult(final Iterator<KeyValue<Serializable, Long>> keyValues) {
        final Map<Serializable, Long> count = new HashMap<>();
        keyValues.forEachRemaining(pair -> count.put(pair.getKey(), pair.getValue()));
        return count;
    }

    @Override
    public String getMemoryKey() {
        return this.memoryKey;
    }

    @Override
    public String toString() {
        return StringFactory.mapReduceString(this, this.memoryKey);
    }

    @Override
    public MapReduce<Serializable, Long, Serializable, Long, Map<Serializable, Long>> clone() {
        try {
            final CountMapReduce clone = (CountMapReduce) super.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(ErrorMessage.CLONE_FAILED.getMessage(this.getClass().toString(), e.getMessage()), e);
        }
    }
}
