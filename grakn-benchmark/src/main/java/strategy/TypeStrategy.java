package strategy;

import ai.grakn.concept.Type;
import pdf.PDF;

//public class TypeStrategy<T extends Type, P extends PDF> extends Strategy{
public class TypeStrategy<T extends Type>{
    private final T type;
//    private final P numInstancesPDF;
    private final PDF numInstancesPDF;

    public TypeStrategy(T type, PDF numInstancesPDF){
        this.type = type;
        this.numInstancesPDF = numInstancesPDF;
    }

    public T getType() {
        return type;
    }

    public PDF getNumInstancesPDF() {
        return numInstancesPDF;
    }
}

