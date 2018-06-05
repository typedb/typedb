package strategy;

import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;

import java.util.Set;

public class RelationshipRoleStrategy {

    private final Role role;
    private Set<RoleTypeStrategy> roleTypeStrategies;
//    private Boolean isPrimary;

    public RelationshipRoleStrategy(Role role, Set<RoleTypeStrategy> roleTypeStrategies, Boolean isPrimary) {
        this.role = role;
        this.roleTypeStrategies = roleTypeStrategies;
//        this.isPrimary = isPrimary;
    }

    public RelationshipRoleStrategy(Role role, RoleTypeStrategy roleTypeStrategy, Boolean isPrimary) {
        this.role = role;
        this.roleTypeStrategies = null;
//        this.isPrimary = isPrimary;
    }

    public RelationshipRoleStrategy(Role role, RoleTypeStrategy roleTypeStrategy) {
        this.role = role;
        this.roleTypeStrategies = null;
//        this.isPrimary = false;
    }

}
