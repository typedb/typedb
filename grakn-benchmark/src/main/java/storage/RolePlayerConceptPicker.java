package storage;

import ai.grakn.GraknTx;
import pdf.PDF;

import java.util.Random;
import java.util.stream.Stream;

public class RolePlayerConceptPicker implements RolePlayerConceptPickerInterface {

    public RolePlayerConceptPicker(Random rand, String typeLabel, String relationshipLabel, String roleLabel, ConceptTypeCountStore conceptTypeCountStore){

    }

    @Override
    public Stream<String> get(PDF pdf, GraknTx tx) {
        return null;
    }

    @Override
    public void reset() {

    }
}
