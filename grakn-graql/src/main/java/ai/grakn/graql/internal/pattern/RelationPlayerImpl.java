package ai.grakn.graql.internal.pattern;

import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.admin.VarAdmin;

import java.util.Optional;

/**
 * A pair of role type and role player (where the role type may not be present)
 */
class RelationPlayerImpl implements RelationPlayer {
    private final Optional<VarAdmin> roleType;
    private final VarAdmin rolePlayer;

    /**
     * A casting without a role type specified
     * @param rolePlayer the role player of the casting
     */
    RelationPlayerImpl(VarAdmin rolePlayer) {
        this.roleType = Optional.empty();
        this.rolePlayer = rolePlayer;
    }

    /**
     * @param roletype the role type of the casting
     * @param rolePlayer the role player of the casting
     */
    RelationPlayerImpl(VarAdmin roletype, VarAdmin rolePlayer) {
        this.roleType = Optional.of(roletype);
        this.rolePlayer = rolePlayer;
    }

    @Override
    public Optional<VarAdmin> getRoleType() {
        return roleType;
    }

    @Override
    public VarAdmin getRolePlayer() {
        return rolePlayer;
    }

    @Override
    public String toString() {
        return getRoleType().map(r -> r.getPrintableName() + ": ").orElse("") + getRolePlayer().getPrintableName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RelationPlayerImpl casting = (RelationPlayerImpl) o;

        if (!roleType.equals(casting.roleType)) return false;
        return rolePlayer.equals(casting.rolePlayer);

    }

    @Override
    public int hashCode() {
        int result = roleType.hashCode();
        result = 31 * result + rolePlayer.hashCode();
        return result;
    }
}
