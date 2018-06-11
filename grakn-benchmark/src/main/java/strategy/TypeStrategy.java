package strategy;

import ai.grakn.concept.Type;
import pdf.PDF;

//public class TypeStrategy<T extends Type, P extends PDF> extends Strategy{
public class TypeStrategy<T extends Type>{
    private final T type;
    private final String typeLabel;
//    private final P numInstancesPDF;
    private final PDF numInstancesPDF;

    public TypeStrategy(T type, PDF numInstancesPDF){
        this.type = type;
        this.numInstancesPDF = numInstancesPDF;
        // TODO Storing the label value can be avoided when TP functionality #20179 is complete
        this.typeLabel = this.type.getLabel().getValue();
    }

    public T getType() {
        return type;
    }

    public String getTypeLabel() {
        return typeLabel;
    }

    public PDF getNumInstancesPDF() {
        return numInstancesPDF;
    }
}

