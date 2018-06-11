package strategy;

import ai.grakn.concept.Role;

import java.util.HashSet;
import java.util.Set;

public class RelationshipRoleStrategy {

    private final Role role;

    private final String roleLabel;

    private Set<RolePlayerTypeStrategy> rolePlayerTypeStrategies;

    public RelationshipRoleStrategy(Role role, Set<RolePlayerTypeStrategy> rolePlayerTypeStrategies) {
        this.role = role;
        this.rolePlayerTypeStrategies = rolePlayerTypeStrategies;
        this.roleLabel = role.getLabel().getValue();
    }

    public RelationshipRoleStrategy(Role role, RolePlayerTypeStrategy rolePlayerTypeStrategy) {
        this.role = role;
        this.rolePlayerTypeStrategies = new HashSet<RolePlayerTypeStrategy>();
        this.rolePlayerTypeStrategies.add(rolePlayerTypeStrategy);
        this.roleLabel = role.getLabel().getValue();
    }

    public Role getRole() {
        return role;
    }

    public String getRoleLabel() {
        return roleLabel;
    }

//    public RelationshipRoleStrategy(Role role, RolePlayerTypeStrategy rolePlayerTypeStrategy) {
//        this.role = role;
//        this.rolePlayerTypeStrategies = new HashSet<RolePlayerTypeStrategy>();
//        this.rolePlayerTypeStrategies.add(rolePlayerTypeStrategy);
//        this.roleLabel = role.getLabel().getValue();
//    }

    public Set<RolePlayerTypeStrategy> getRolePlayerTypeStrategies() {
        return rolePlayerTypeStrategies;
    }
}
