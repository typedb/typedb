package ai.grakn.util;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.remote.RemoteGraknTx;
import ai.grakn.remote.concept.RemoteConcepts;
import ai.grakn.rpc.util.TxConceptReader;
import ai.grakn.rpc.generated.GrpcConcept;

public class RemoteConceptReader extends TxConceptReader {

    private final RemoteGraknTx tx;

    public RemoteConceptReader(RemoteGraknTx tx){
        this.tx = tx;
    }

    @Override
    public Concept concept(GrpcConcept.Concept concept) {
        ConceptId id = ConceptId.of(concept.getId().getValue());

        switch (concept.getBaseType()) {
            case Entity:
                return RemoteConcepts.createEntity(tx, id);
            case Relationship:
                return RemoteConcepts.createRelationship(tx, id);
            case Attribute:
                return RemoteConcepts.createAttribute(tx, id);
            case EntityType:
                return RemoteConcepts.createEntityType(tx, id);
            case RelationshipType:
                return RemoteConcepts.createRelationshipType(tx, id);
            case AttributeType:
                return RemoteConcepts.createAttributeType(tx, id);
            case Role:
                return RemoteConcepts.createRole(tx, id);
            case Rule:
                return RemoteConcepts.createRule(tx, id);
            case MetaType:
                return RemoteConcepts.createMetaType(tx, id);
            default:
            case UNRECOGNIZED:
                throw new IllegalArgumentException("Unrecognised " + concept);
        }
    }
}
