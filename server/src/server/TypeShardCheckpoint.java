package grakn.core.server;

import grakn.core.concept.Concept;
import grakn.core.concept.Label;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.concept.ConceptVertex;
import grakn.core.server.session.TransactionOLTP;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Optional;

public class TypeShardCheckpoint {
    public void set(TransactionOLTP tx, Label label, long checkpoint) {
        Concept schemaConcept = tx.getSchemaConcept(label);
        if (schemaConcept != null) {
            Vertex janusVertex = ConceptVertex.from(schemaConcept).vertex().element();
            janusVertex.property(Schema.VertexProperty.TYPE_SHARD_LAST_CHECKPOINT.name(), checkpoint);
        }
        else {
            throw new RuntimeException("Label '" + label.getValue() + "' does not exist");
        }
    }

    public Optional<Long> get(TransactionOLTP tx, Label label) {
        Concept schemaConcept = tx.getSchemaConcept(label);
        if (schemaConcept != null) {
            Vertex janusVertex = ConceptVertex.from(schemaConcept).vertex().element();
            VertexProperty<Object> property = janusVertex.property(Schema.VertexProperty.TYPE_SHARD_LAST_CHECKPOINT.name());
            return Optional.of((Long) property.orElse(0L));
        }
        else {
            return Optional.empty();
        }
    }
}
