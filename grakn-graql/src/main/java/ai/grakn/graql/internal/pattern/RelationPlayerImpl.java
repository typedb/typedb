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

package ai.grakn.graql.internal.pattern;

import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.admin.VarPatternAdmin;

import java.util.Optional;

/**
 * A pair of role type and role player (where the role type may not be present)
 */
class RelationPlayerImpl implements RelationPlayer {
    private final int hashCode;
    private final Optional<VarPatternAdmin> roleType;
    private final VarPatternAdmin rolePlayer;

    /**
     * @param roletype the role type of the casting
     * @param rolePlayer the role player of the casting
     */
    private RelationPlayerImpl(Optional<VarPatternAdmin> roletype, VarPatternAdmin rolePlayer) {
        this.roleType = roletype;
        this.rolePlayer = rolePlayer;
        hashCode = 31 * roleType.hashCode() + rolePlayer.hashCode();
    }

    /**
     * A casting without a role type specified
     * @param rolePlayer the role player of the casting
     */
    static RelationPlayerImpl of(VarPatternAdmin rolePlayer) {
        return new RelationPlayerImpl(Optional.empty(), rolePlayer);
    }

    /**
     * @param roleType the role type of the casting
     * @param rolePlayer the role player of the casting
     */
    static RelationPlayerImpl of(VarPatternAdmin roleType, VarPatternAdmin rolePlayer) {
        return new RelationPlayerImpl(Optional.of(roleType), rolePlayer);
    }

    @Override
    public Optional<VarPatternAdmin> getRoleType() {
        return roleType;
    }

    @Override
    public VarPatternAdmin getRolePlayer() {
        return rolePlayer;
    }

    @Override
    public RelationPlayer setRolePlayer(VarPatternAdmin rolePlayer) {
        return new RelationPlayerImpl(roleType, rolePlayer);
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

        return roleType.equals(casting.roleType) && rolePlayer.equals(casting.rolePlayer);

    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
