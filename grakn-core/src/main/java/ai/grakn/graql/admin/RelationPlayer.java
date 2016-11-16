package ai.grakn.graql.admin;

import java.util.Optional;

/**
 * A pair of role type and role player (where the role type may not be present)
 */
public interface RelationPlayer {
    /**
     * @return the role type, if specified
     */
    Optional<VarAdmin> getRoleType();

    /**
     * @return the role player
     */
    VarAdmin getRolePlayer();
}
