package strategy;

import ai.grakn.concept.AttributeType;
import pdf.DiscreteGaussianPDF;
import pdf.PDF;

import java.util.Set;

//public class AttributeStrategy <P extends PDF> extends TypeStrategy<AttributeType, P> {
public class AttributeStrategy extends TypeStrategy<AttributeType> {

    Set<TypeStrategy> ownerStrategies;

    public AttributeStrategy(AttributeType type, PDF numInstancesPDF, Set<TypeStrategy> ownerStrategies) {
        super(type, numInstancesPDF);
        this.ownerStrategies = ownerStrategies;
    }

//    public <P extends PDF> AttributeStrategy(AttributeType type, P numInstancesPDF, double frequency, Random rand) {
//    public AttributeStrategy(AttributeType type, PDF numInstancesPDF) {
//    }
}
