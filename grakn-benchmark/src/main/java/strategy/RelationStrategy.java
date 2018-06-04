package strategy;

import ai.grakn.concept.RelationshipType;

import java.util.Random;
import java.util.Set;

public class RelationStrategy extends TypeStrategy<RelationshipType> {

    Set<RoleStrategy> roleStrategies;

    public RelationStrategy(RelationshipType type, DiscreteGaussianPDF numInstancesPDF, double frequency, Random rand) {
        super(type, numInstancesPDF, frequency, rand);
    }
}
