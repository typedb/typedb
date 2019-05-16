package grakn.core.server.statistics;

import grakn.core.concept.Concept;
import grakn.core.concept.Label;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.concept.ConceptVertex;
import grakn.core.server.session.TransactionOLTP;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KeyspaceStatistics {

    private ConcurrentHashMap<String, Long> instanceCountsCache;

    public KeyspaceStatistics() {
        instanceCountsCache = new ConcurrentHashMap<>();
    }

    public long count(TransactionOLTP tx, String label) {
        // return count if cached, else cache miss and retrieve from Janus
        instanceCountsCache.computeIfAbsent(label, l -> retrieveCountFromVertex(tx, l));
        return instanceCountsCache.get(label);
    }

    public void commit(TransactionOLTP tx, UncomittedStatisticsDelta statisticsDelta) {
        HashMap<String, Long> deltaMap = statisticsDelta.instanceDeltas();

        // merge each delta into the cache, then flush the cache to Janus
        for (Map.Entry<String, Long> entry : deltaMap.entrySet()) {
            String label = entry.getKey();
            Long delta = entry.getValue();
            instanceCountsCache.compute(label, (k, prior) -> {
                long existingCount;
                if (instanceCountsCache.containsKey(label)) {
                    existingCount = prior;
                } else {
                    existingCount = retrieveCountFromVertex(tx, label) ;
                }
                return existingCount + delta;
            });
        }

        persist(tx);
    }

    private void persist(TransactionOLTP tx) {
        for (String label : instanceCountsCache.keySet()) {
            Long count = instanceCountsCache.get(label);
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
