package ai.grakn.graql.internal.analytics;

import ai.grakn.util.Schema;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.util.*;

class DegreeDistributionMapReduce extends GraknMapReduce<Set<String>> {

    public DegreeDistributionMapReduce() {
    }

    public DegreeDistributionMapReduce(Set<String> selectedTypes) {
        this.selectedTypes = selectedTypes;
    }

    @Override
    public void safeMap(final Vertex vertex, final MapEmitter<Serializable, Set<String>> emitter) {
        if (selectedTypes.contains(Utility.getVertexType(vertex))) {
            emitter.emit(vertex.value(DegreeVertexProgram.DEGREE),
                    Collections.singleton(vertex.value(Schema.ConceptProperty.ITEM_IDENTIFIER.name())));
        }
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

    @Override
    public boolean doStage(Stage stage) {
        return true;
    }

    @Override
    public Map<Serializable, Set<String>> generateFinalResult(Iterator<KeyValue<Serializable, Set<String>>> keyValues) {
        final Map<Serializable, Set<String>> clusterPopulation = new HashMap<>();
        keyValues.forEachRemaining(pair -> clusterPopulation.put(pair.getKey(), pair.getValue()));
        return clusterPopulation;
    }
}
