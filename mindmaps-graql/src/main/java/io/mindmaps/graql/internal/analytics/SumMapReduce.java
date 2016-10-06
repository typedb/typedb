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

public class SumMapReduce extends MindmapsMapReduce<Number> {

    public static final String MEMORY_KEY = "sum";
    private static final String RESOURCE_DATA_TYPE_KEY = "RESOURCE_DATA_TYPE_KEY";

    public SumMapReduce() {
    }

    public SumMapReduce(Set<String> selectedTypes, String resourceDataType) {
        this.selectedTypes = selectedTypes;
        persistentProperties.put(RESOURCE_DATA_TYPE_KEY, resourceDataType);
    }

    @Override
    public void safeMap(final Vertex vertex, final MapEmitter<Serializable, Number> emitter) {
        if (persistentProperties.get(RESOURCE_DATA_TYPE_KEY).equals(ResourceType.DataType.LONG.getName())) {
            if (selectedTypes.contains(getVertexType(vertex)) &&
                    ((Long) vertex.value(DegreeVertexProgram.MEMORY_KEY)) > 0) {
                emitter.emit(MEMORY_KEY,
                        ((Long) vertex.value(Schema.ConceptProperty.VALUE_LONG.name())) *
                                ((Long) vertex.value(DegreeVertexProgram.MEMORY_KEY)));
                return;
            }
            emitter.emit(MEMORY_KEY, 0L);
        } else {
            if (selectedTypes.contains(getVertexType(vertex)) &&
                    ((Long) vertex.value(DegreeVertexProgram.MEMORY_KEY)) > 0) {
                emitter.emit(MEMORY_KEY,
                        ((Double) vertex.value(Schema.ConceptProperty.VALUE_DOUBLE.name())) *
                                ((Long) vertex.value(DegreeVertexProgram.MEMORY_KEY)));
                return;
            }
            emitter.emit(MEMORY_KEY, 0D);
        }
    }

    @Override
    public void reduce(final Serializable key, final Iterator<Number> values,
                       final ReduceEmitter<Serializable, Number> emitter) {
        if (persistentProperties.get(RESOURCE_DATA_TYPE_KEY).equals(ResourceType.DataType.LONG.getName())) {
            emitter.emit(key, IteratorUtils.reduce(values, 0L,
                    (a, b) -> a.longValue() + b.longValue()));
        } else {
            emitter.emit(key, IteratorUtils.reduce(values, 0D,
                    (a, b) -> a.doubleValue() + b.doubleValue()));
        }
    }

    @Override
    public void combine(final Serializable key, final Iterator<Number> values,
                        final ReduceEmitter<Serializable, Number> emitter) {
        this.reduce(key, values, emitter);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public Map<Serializable, Number> generateFinalResult(Iterator<KeyValue<Serializable, Number>> keyValues) {
        final Map<Serializable, Number> sum = new HashMap<>();
        keyValues.forEachRemaining(pair -> sum.put(pair.getKey(), pair.getValue()));
        return sum;
    }
}
