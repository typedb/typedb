package pick;

import ai.grakn.GraknTx;
import storage.ConceptTypeCountStore;

import java.util.Random;

import static ai.grakn.graql.Graql.var;

public class IsaTypeConceptIdPicker extends ConceptIdPicker {

    private ConceptTypeCountStore conceptTypeCountStore;
    private String typeLabel;

    public IsaTypeConceptIdPicker(Random rand, ConceptTypeCountStore conceptTypeCountStore, String typeLabel) {
        super(rand, var("x").isa(typeLabel), var("x"));
        this.conceptTypeCountStore = conceptTypeCountStore;
        this.typeLabel = typeLabel;
    }

    private Integer getConceptCount(GraknTx tx) {
        return conceptTypeCountStore.get(this.typeLabel);
    }
}
