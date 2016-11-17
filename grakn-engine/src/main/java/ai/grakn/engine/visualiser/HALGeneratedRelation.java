package ai.grakn.engine.visualiser;

import com.theoryinpractise.halbuilder.api.Representation;
import com.theoryinpractise.halbuilder.api.RepresentationFactory;
import com.theoryinpractise.halbuilder.standard.StandardRepresentationFactory;

public class HALGeneratedRelation {

    private RepresentationFactory factory;

    private final static String ONTOLOGY_LINK = "ontology";
    private final static String INBOUND_EDGE = "IN";

    // - State properties

    private final static String ID_PROPERTY = "_id";
    private final static String TYPE_PROPERTY = "_type";
    private final static String BASETYPE_PROPERTY = "_baseType";
    private final static String DIRECTION_PROPERTY = "_direction";

    public HALGeneratedRelation() {
        this.factory = new StandardRepresentationFactory();
    }

    public Representation getNewGeneratedRelation(String assertionID, String relationType) {
        Representation assertionResource = factory.newRepresentation(assertionID)
                .withProperty(ID_PROPERTY, "temp-assertion")
                .withProperty(TYPE_PROPERTY, relationType)
                .withProperty(BASETYPE_PROPERTY, "generated-relation")
                .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE)
                .withLink(ONTOLOGY_LINK, "");
        return assertionResource;
    }
}
