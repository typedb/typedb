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
    public static final String SELECTED_DATA_TYPE = "SELECTED_DATA_TYPE";

    public static final String COUNT = "C";
    public static final String SUM = "S";

    private String resourceDataType = null;

    //Needed internally for OLAP tasks
    public MeanMapReduce() {
    }

    public MeanMapReduce(Set<String> selectedTypes, Map<String, String> resourceTypes) {
        this.selectedTypes = selectedTypes;
        resourceDataType = resourceTypes.get(selectedTypes.iterator().next());
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);
        configuration.addProperty(SELECTED_DATA_TYPE, resourceDataType);
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        super.loadState(graph, configuration);
        resourceDataType = configuration.getString(SELECTED_DATA_TYPE);
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<Serializable, Map<String, Number>> emitter) {
        if (resourceDataType.equals(ResourceType.DataType.LONG.getName())) {
            if (selectedTypes.contains(getVertexType(vertex))) {
                Map<String, Number> pair = new HashMap<>(2);
                pair.put(SUM, vertex.value(Schema.ConceptProperty.VALUE_LONG.name()));
                pair.put(COUNT, 1L);
                emitter.emit(MEMORY_KEY, pair);
                return;
            }
            Map<String, Number> emptyPair = new HashMap<>(2);
            emptyPair.put(SUM, 0);
            emptyPair.put(COUNT, 0);
            emitter.emit(MEMORY_KEY, emptyPair);
        } else {
            if (selectedTypes.contains(getVertexType(vertex))) {
                Map<String, Number> pair = new HashMap<>(2);
                pair.put(SUM, vertex.value(Schema.ConceptProperty.VALUE_DOUBLE.name()));
                pair.put(COUNT, 1L);
                emitter.emit(MEMORY_KEY, pair);
                return;
            }
            Map<String, Number> emptyPair = new HashMap<>(2);
            emptyPair.put(SUM, 0);
            emptyPair.put(COUNT, 0);
            emitter.emit(MEMORY_KEY, emptyPair);
        }
    }

    @Override
    public void reduce(final Serializable key, final Iterator<Map<String, Number>> values,
                       final ReduceEmitter<Serializable, Map<String, Number>> emitter) {
        Map<String, Number> emptyPair = new HashMap<>(2);
        emptyPair.put(SUM, 0);
        emptyPair.put(COUNT, 0);
        emitter.emit(key, IteratorUtils.reduce(values, emptyPair,
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
