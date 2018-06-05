package strategy;

import ai.grakn.concept.Type;
import pdf.PDF;

public class RoleTypeStrategy extends TypeStrategy {

    private final Boolean isCentral;

    public RoleTypeStrategy(Type type, PDF numInstancesPDF, Boolean isCentral) {
        super(type, numInstancesPDF);
        this.isCentral = isCentral;
    }

    public RoleTypeStrategy(Type type, PDF numInstancesPDF) {
        super(type, numInstancesPDF);
        this.isCentral = false;
    }

    public Boolean getCentral() {
        return isCentral;
    }
}

//import pdf.PDF;

//public class RoleTypeStrategy extends TypeStrategy {
//
//    private PDF valuesPDF;
//
//    public RoleTypeStrategy(PDF valuesPDF) {
//        super();
//        this.valuesPDF = valuesPDF;
//    }
//
//    public PDF getValuesPDF() {
//        return valuesPDF;
//    }
//}
