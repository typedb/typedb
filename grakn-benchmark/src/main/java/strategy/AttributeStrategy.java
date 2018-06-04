package strategy;

import ai.grakn.concept.AttributeType;

import java.util.Random;
import java.util.Set;

public class AttributeStrategy extends TypeStrategy<AttributeType> {

    Set<TypeStrategy> ownerStrategies;

    public AttributeStrategy(AttributeType type, DiscreteGaussianPDF numInstancesPDF, double frequency, Random rand) {
        super(type, numInstancesPDF, frequency, rand);
    }
}
