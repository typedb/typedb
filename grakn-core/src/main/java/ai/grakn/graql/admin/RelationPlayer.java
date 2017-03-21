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
 * A pair of role type and role player (where the role type may not be present)
 *
 * @author Felix Chapman
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

    // TODO: If `VarAdmin#setVarName` is removed, this may no longer be necessary
    /**
     * Set the role player, returning a new {@link RelationPlayer} with that role player set
     */
    @CheckReturnValue
    RelationPlayer setRolePlayer(VarAdmin rolePlayer);
}
