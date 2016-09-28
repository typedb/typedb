package io.mindmaps.graql.internal.analytics;

import io.mindmaps.concept.ResourceType;
import io.mindmaps.util.Schema;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class MeanMapReduce extends MindmapsMapReduce<Map<String, Number>> {

    public static final String MEMORY_KEY = "mean";
    private static final String RESOURCE_TYPE_KEY = "RESOURCE_TYPE_KEY";
    private static final String RESOURCE_DATA_TYPE_KEY = "RESOURCE_DATA_TYPE_KEY";

    public static final String COUNT = "C";
    public static final String SUM = "S";

    public MeanMapReduce() {
    }

    public MeanMapReduce(Set<String> selectedTypes, String resourceDataType) {
        this.selectedTypes = selectedTypes;
        String resourceDataTypeValue = resourceDataType.equals(ResourceType.DataType.LONG.getName()) ?
                Schema.ConceptProperty.VALUE_LONG.name() : Schema.ConceptProperty.VALUE_DOUBLE.name();
        persistentProperties.put(RESOURCE_DATA_TYPE_KEY, resourceDataTypeValue);
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<Serializable, Map<String, Number>> emitter) {
        if (selectedTypes.contains(getVertexType(vertex))) {
            Map<String, Number> tuple = new HashMap<>(2);
            tuple.put(SUM, vertex.value((String) persistentProperties.get(RESOURCE_DATA_TYPE_KEY)));
            tuple.put(COUNT, 1L);
            emitter.emit(MEMORY_KEY, tuple);
            return;
        }
        Map<String, Number> emptyTuple = new HashMap<>(2);
        emptyTuple.put(SUM, 0);
        emptyTuple.put(COUNT, 0);
        emitter.emit(MEMORY_KEY, emptyTuple);
    }

    @Override
    public void reduce(final Serializable key, final Iterator<Map<String, Number>> values,
                       final ReduceEmitter<Serializable, Map<String, Number>> emitter) {
        Map<String, Number> emptyTuple = new HashMap<>(2);
        emptyTuple.put(SUM, 0);
        emptyTuple.put(COUNT, 0);
        emitter.emit(key, IteratorUtils.reduce(values, emptyTuple,
                (a, b) -> {
                    a.put(COUNT, a.get(COUNT).longValue() + b.get(COUNT).longValue());
                    a.put(SUM, a.get(SUM).doubleValue() + b.get(SUM).doubleValue());
                    return a;
                }));
    }

    @Override
    public void combine(final Serializable key, final Iterator<Map<String, Number>> values,
                        final ReduceEmitter<Serializable, Map<String, Number>> emitter) {
        this.reduce(key, values, emitter);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public Map<Serializable, Map<String, Number>> generateFinalResult(
            Iterator<KeyValue<Serializable, Map<String, Number>>> keyValues) {
        final Map<Serializable, Map<String, Number>> mean = new HashMap<>();
        keyValues.forEachRemaining(pair -> mean.put(pair.getKey(), pair.getValue()));
        return mean;
    }
}
