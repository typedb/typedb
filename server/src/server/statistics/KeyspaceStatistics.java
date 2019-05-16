package grakn.core.server.statistics;

import grakn.core.concept.Concept;
import grakn.core.concept.Label;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.concept.ConceptVertex;
import grakn.core.server.session.TransactionOLTP;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class KeyspaceStatistics {

    private ConcurrentHashMap<String, Long> instanceCounts;

    public KeyspaceStatistics() {
        instanceCounts = new ConcurrentHashMap<>();
    }

    public long count(TransactionOLTP tx, String label) {
        instanceCounts.computeIfAbsent(label, l -> retrieveCountFromVertex(tx, l));
        return instanceCounts.get(label);
    }

    public void commit(TransactionOLTP tx, UncomittedStatisticsDelta statisticsDelta) {
        HashMap<String, Long> deltaMap = statisticsDelta.instanceDeltas();
        deltaMap.forEach((key, delta) -> instanceCounts.compute(key, (k, prior) -> {
            if (instanceCounts.containsKey(key)) {
                return prior + delta;
            } else {
                return retrieveCountFromVertex(tx, key) + delta;
            }
        }));
        persist(tx);
    }

    private void persist(TransactionOLTP tx) {
        for (String label : instanceCounts.keySet()) {
            Long count = instanceCounts.get(label);
            Concept schemaConcept = tx.getSchemaConcept(Label.of(label));
            Vertex janusVertex = ConceptVertex.from(schemaConcept).vertex().element();
            janusVertex.property(Schema.VertexProperty.INSTANCE_COUNT.name(), count);
        }
    }

    private long retrieveCountFromVertex(TransactionOLTP tx, String label) {
        Concept schemaConcept = tx.getSchemaConcept(Label.of(label));
        Vertex janusVertex = ConceptVertex.from(schemaConcept).vertex().element();
        VertexProperty<Object> property = janusVertex.property(Schema.VertexProperty.INSTANCE_COUNT.name());
        // VertexProperty is similar to a Java Optional
        return (Long) property.orElse(0L);
    }
}
