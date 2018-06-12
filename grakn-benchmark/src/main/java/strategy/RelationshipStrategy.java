package strategy;

import ai.grakn.concept.RelationshipType;
import pdf.PDF;

import java.util.Set;

public class RelationshipStrategy extends TypeStrategy<RelationshipType> {

    private Set<RolePlayerTypeStrategy> rolePlayerTypeStrategies;

    public <P extends PDF> RelationshipStrategy(RelationshipType type, P numInstancesPDF, Set<RolePlayerTypeStrategy> rolePlayerTypeStrategies) {
        super(type, numInstancesPDF);
        this.rolePlayerTypeStrategies = rolePlayerTypeStrategies;
    }

    public Set<RolePlayerTypeStrategy> getRolePlayerTypeStrategies() {
        return rolePlayerTypeStrategies;
    }
}
