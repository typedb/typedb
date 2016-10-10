package io.mindmaps.graql.internal.analytics;

import io.mindmaps.concept.ResourceType;
import io.mindmaps.util.Schema;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class MeanMapReduce extends MindmapsMapReduce<Map<String, Double>> {

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
    public void safeMap(final Vertex vertex, final MapEmitter<Serializable, Map<String, Double>> emitter) {
        if (selectedTypes.contains(getVertexType(vertex)) &&
                ((Long) vertex.value(DegreeVertexProgram.MEMORY_KEY)) > 0) {
            Map<String, Double> tuple = new HashMap<>(2);
            Double degree = ((Long)vertex.value(DegreeVertexProgram.MEMORY_KEY)).doubleValue();
            tuple.put(SUM, degree *
                    ((Number) vertex.value((String) persistentProperties.get(RESOURCE_DATA_TYPE_KEY))).doubleValue());
            tuple.put(COUNT, degree);
            emitter.emit(MEMORY_KEY, tuple);
            return;
        }
        Map<String, Double> emptyTuple = new HashMap<>(2);
        emptyTuple.put(SUM, 0D);
        emptyTuple.put(COUNT, 0D);
        emitter.emit(MEMORY_KEY, emptyTuple);
    }

    @Override
    public void reduce(final Serializable key, final Iterator<Map<String, Double>> values,
                       final ReduceEmitter<Serializable, Map<String, Double>> emitter) {
        Map<String, Double> emptyTuple = new HashMap<>(2);
        emptyTuple.put(SUM, 0D);
        emptyTuple.put(COUNT, 0D);
        emitter.emit(key, IteratorUtils.reduce(values, emptyTuple,
                (a, b) -> {
                    a.put(COUNT, a.get(COUNT) + b.get(COUNT));
                    a.put(SUM, a.get(SUM) + b.get(SUM));
                    return a;
                }));
    }

    @Override
    public void combine(final Serializable key, final Iterator<Map<String, Double>> values,
                        final ReduceEmitter<Serializable, Map<String, Double>> emitter) {
        this.reduce(key, values, emitter);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public Map<Serializable, Map<String, Double>> generateFinalResult(
            Iterator<KeyValue<Serializable, Map<String, Double>>> keyValues) {
        final Map<Serializable, Map<String, Double>> mean = new HashMap<>();
        keyValues.forEachRemaining(pair -> mean.put(pair.getKey(), pair.getValue()));
        return mean;
    }
}
