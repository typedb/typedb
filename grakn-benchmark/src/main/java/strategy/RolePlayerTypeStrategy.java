package strategy;

import ai.grakn.concept.Type;
import pdf.PDF;
import storage.ConceptPicker;

public class RolePlayerTypeStrategy extends TypeStrategy {

    private final Boolean isCentral;

    private ConceptPicker conceptPicker;
//    private final Boolean isUniquelyPicked;  // For instance we only want to assign employees to a company once

    public RolePlayerTypeStrategy(Type type, PDF numInstancesPDF, ConceptPicker conceptPicker, Boolean isCentral) {
        super(type, numInstancesPDF);
        this.isCentral = isCentral;
        this.conceptPicker = conceptPicker;
    }

    public RolePlayerTypeStrategy(Type type, PDF numInstancesPDF, ConceptPicker conceptPicker) {
        super(type, numInstancesPDF);
        this.isCentral = false;
        this.conceptPicker = conceptPicker;
    }

    public ConceptPicker getConceptPicker() {
        return conceptPicker;
    }

//    public RolePlayerTypeStrategy(Type type, PDF numInstancesPDF, Boolean isCentral, Boolean isUniquelyPicked) {
//        super(type, numInstancesPDF);
//        this.isCentral = isCentral;
////        this.isUniquelyPicked = isUniquelyPicked;
//    }
//
//    public RolePlayerTypeStrategy(Type type, PDF numInstancesPDF) {
//        super(type, numInstancesPDF);
//        this.isCentral = false;
////        this.isUniquelyPicked = false;
//    }

    public Boolean getCentral() {
        return isCentral;
    }
}

//import pdf.PDF;

//public class RolePlayerTypeStrategy extends TypeStrategy {
//
//    private PDF valuesPDF;
//
//    public RolePlayerTypeStrategy(PDF valuesPDF) {
//        super();
//        this.valuesPDF = valuesPDF;
//    }
//
//    public PDF getValuesPDF() {
//        return valuesPDF;
//    }
//}
