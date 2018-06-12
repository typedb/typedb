package strategy;

import ai.grakn.concept.Role;
import ai.grakn.concept.Type;
import pdf.PDF;
import storage.RolePlayerConceptPickerInterface;

public class RolePlayerTypeStrategy extends TypeStrategy {

    private final Role role;
    private final String roleLabel;
    private RolePlayerConceptPickerInterface conceptPicker;

    public RolePlayerTypeStrategy(Role role, Type type, PDF numInstancesPDF, RolePlayerConceptPickerInterface conceptPicker) {
        super(type, numInstancesPDF);
        this.role = role;
        this.roleLabel = role.getLabel().getValue();
        this.conceptPicker = conceptPicker;
    }

    public RolePlayerConceptPickerInterface getConceptPicker() {
        return conceptPicker;
    }

    public String getRoleLabel() {
        return this.roleLabel;
    }

    public Role getRole() {
        return role;
    }
}

