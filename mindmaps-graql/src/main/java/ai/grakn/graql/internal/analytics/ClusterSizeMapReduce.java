package ai.grakn.graql.internal.analytics;

import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

class ClusterSizeMapReduce extends MindmapsMapReduce<Long> {

    private static final String CLUSTER_LABEL = "clusterMemberMapReduce.clusterLabel";

    public ClusterSizeMapReduce() {
    }

    public ClusterSizeMapReduce(Set<String> selectedTypes, String clusterLabel) {
        this.selectedTypes = selectedTypes;
        this.persistentProperties.put(CLUSTER_LABEL, clusterLabel);
    }

    @Override
    public void safeMap(final Vertex vertex, final MapEmitter<Serializable, Long> emitter) {
        if (selectedTypes.contains(Utility.getVertexType(vertex))) {
            emitter.emit(vertex.value((String) persistentProperties.get(CLUSTER_LABEL)), 1L);
        }
    }

    @Override
    public void reduce(final Serializable key, final Iterator<Long> values,
                       final ReduceEmitter<Serializable, Long> emitter) {
        emitter.emit(key, IteratorUtils.reduce(values, 0L,
                (a, b) -> a + b));
    }

    @Override
    public void combine(final Serializable key, final Iterator<Long> values,
                        final ReduceEmitter<Serializable, Long> emitter) {
        this.reduce(key, values, emitter);
    }

    @Override
    public boolean doStage(Stage stage) {
        return true;
    }

    @Override
    public Map<Serializable, Long> generateFinalResult(Iterator<KeyValue<Serializable, Long>> keyValues) {
        final Map<Serializable, Long> clusterPopulation = new HashMap<>();
        keyValues.forEachRemaining(pair -> clusterPopulation.put(pair.getKey(), pair.getValue()));
        return clusterPopulation;
    }
}
