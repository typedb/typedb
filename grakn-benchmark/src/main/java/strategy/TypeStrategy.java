package strategy;

import ai.grakn.concept.Type;

import java.util.Random;

public class TypeStrategy<T extends Type> extends Strategy{
    private final T type;
    private final DiscreteGaussianPDF numInstancesPDF;

    public TypeStrategy(T type, DiscreteGaussianPDF numInstancesPDF, double frequency, Random rand){
        super(frequency, rand);
        this.type = type;
        this.numInstancesPDF = numInstancesPDF;
    }

    public T getType() {
        return type;
    }

    public DiscreteGaussianPDF getNumInstancesPDF() {
        return numInstancesPDF;
    }
}

