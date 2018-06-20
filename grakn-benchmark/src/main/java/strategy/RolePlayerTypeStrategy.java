package strategy;

import ai.grakn.concept.Role;
import ai.grakn.concept.Type;
import pdf.PDF;
import pick.StreamProviderInterface;

public class RolePlayerTypeStrategy extends TypeStrategy implements HasPicker {

    private final Role role;
    private final String roleLabel;
    private StreamProviderInterface conceptPicker;

    public RolePlayerTypeStrategy(Role role, Type type, PDF numInstancesPDF, StreamProviderInterface conceptPicker) {
        super(type, numInstancesPDF);
        this.role = role;
        this.roleLabel = role.getLabel().getValue();
        this.conceptPicker = conceptPicker;
    }

    public StreamProviderInterface getConceptPicker() {
         return conceptPicker;
    }

    public String getRoleLabel() {
        return this.roleLabel;
    }

    public Role getRole() {
        return role;
    }
}

