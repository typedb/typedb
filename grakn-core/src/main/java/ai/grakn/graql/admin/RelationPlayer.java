/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.admin;

import javax.annotation.CheckReturnValue;
import java.util.Optional;

/**
 * A pair of role and role player (where the role may not be present)
 *
 * @author Felix Chapman
 */
public class RelationPlayer {
    private final int hashCode;
    private final Optional<VarPatternAdmin> role;
    private final VarPatternAdmin rolePlayer;

    /**
     * @param role the role of the role - role player pair
     * @param rolePlayer the role player of the  role - role player pair
     */
    private RelationPlayer(Optional<VarPatternAdmin> role, VarPatternAdmin rolePlayer) {
        this.role = role;
        this.rolePlayer = rolePlayer;
        hashCode = 31 * this.role.hashCode() + rolePlayer.hashCode();
    }

    /**
     * A role - role player pair without a role specified
     * @param rolePlayer the role player of the role - role player pair
     */
    public static RelationPlayer of(VarPatternAdmin rolePlayer) {
        return new RelationPlayer(Optional.empty(), rolePlayer);
    }

    /**
     * @param role the role of the role - role player pair
     * @param rolePlayer the role player of the role - role player pair
     */
    public static RelationPlayer of(VarPatternAdmin role, VarPatternAdmin rolePlayer) {
        return new RelationPlayer(Optional.of(role), rolePlayer);
    }

    /**
     * @return the role, if specified
     */
    @CheckReturnValue
    public Optional<VarPatternAdmin> getRole() {
        return role;
    }

    /**
     * @return the role player
     */
    @CheckReturnValue
    public VarPatternAdmin getRolePlayer() {
        return rolePlayer;
    }

    @Override
    public String toString() {
        return getRole().map(r -> r.getPrintableName() + ": ").orElse("") + getRolePlayer().getPrintableName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RelationPlayer casting = (RelationPlayer) o;

        return role.equals(casting.role) && rolePlayer.equals(casting.rolePlayer);

    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
