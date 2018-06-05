package strategy;

import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Type;
import pdf.DiscreteGaussianPDF;
import pdf.PDF;

import java.util.Set;

public class RelationshipStrategy extends TypeStrategy<RelationshipType> {

    private Set<RelationshipRoleStrategy> roleStrategies;

    public <P extends PDF> RelationshipStrategy(RelationshipType type, P numInstancesPDF, Set<RelationshipRoleStrategy> roleStrategies) {
        super(type, numInstancesPDF);
        this.roleStrategies = roleStrategies;
    }
}
