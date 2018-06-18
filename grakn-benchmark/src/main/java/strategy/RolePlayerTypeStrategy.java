package strategy;

import ai.grakn.concept.Role;
import ai.grakn.concept.Type;
import pdf.PDF;
import pick.ConceptIdStreamLimiterInterface;

public class RolePlayerTypeStrategy extends TypeStrategy {

    private final Role role;
    private final String roleLabel;
    private ConceptIdStreamLimiterInterface conceptPicker;

    public RolePlayerTypeStrategy(Role role, Type type, PDF numInstancesPDF, ConceptIdStreamLimiterInterface conceptPicker) {
        super(type, numInstancesPDF);
        this.role = role;
        this.roleLabel = role.getLabel().getValue();
        this.conceptPicker = conceptPicker;
    }

    public ConceptIdStreamLimiterInterface getConceptPicker() {
         return conceptPicker;
    }

    public String getRoleLabel() {
        return this.roleLabel;
    }

    public Role getRole() {
        return role;
    }
}

