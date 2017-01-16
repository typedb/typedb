package ai.grakn.graql.internal.hal;

import ai.grakn.concept.TypeName;
import com.theoryinpractise.halbuilder.api.Representation;
import com.theoryinpractise.halbuilder.api.RepresentationFactory;
import com.theoryinpractise.halbuilder.standard.StandardRepresentationFactory;

class HALGeneratedRelation {

    private final RepresentationFactory factory;

    private final static String ONTOLOGY_LINK = "ontology";
    private final static String INBOUND_EDGE = "IN";

    // - State properties

    private final static String ID_PROPERTY = "_id";
    private final static String TYPE_PROPERTY = "_type";
    private final static String BASETYPE_PROPERTY = "_baseType";
    private final static String DIRECTION_PROPERTY = "_direction";

    HALGeneratedRelation() {
        this.factory = new StandardRepresentationFactory();
    }

    Representation getNewGeneratedRelation(String assertionID, TypeName relationType) {
        return factory.newRepresentation(assertionID)
                .withProperty(ID_PROPERTY, "temp-assertion")
                .withProperty(TYPE_PROPERTY, relationType.getValue())
                .withProperty(BASETYPE_PROPERTY, "generated-relation")
                .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE)
                .withLink(ONTOLOGY_LINK, "");
    }
}
