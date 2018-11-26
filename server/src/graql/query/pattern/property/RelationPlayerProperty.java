/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.query.pattern.property;

import com.google.auto.value.AutoValue;
import grakn.core.graql.query.pattern.Statement;

import javax.annotation.CheckReturnValue;
import java.util.Optional;

/**
 * A pair of role and role player (where the role may not be present)
 *
 */
@AutoValue
public abstract class RelationPlayerProperty {

    /**
     * A role - role player pair without a role specified
     * @param rolePlayer the role player of the role - role player pair
     */
    public static RelationPlayerProperty of(Statement rolePlayer) {
        return new AutoValue_RelationPlayerProperty(Optional.empty(), rolePlayer);
    }

    /**
     * @param role the role of the role - role player pair
     * @param rolePlayer the role player of the role - role player pair
     */
    public static RelationPlayerProperty of(Statement role, Statement rolePlayer) {
        return new AutoValue_RelationPlayerProperty(Optional.of(role), rolePlayer);
    }

    /**
     * @return the role, if specified
     */
    @CheckReturnValue
    public abstract Optional<Statement> getRole();

    /**
     * @return the role player
     */
    @CheckReturnValue
    public abstract Statement getRolePlayer();

    @Override
    public String toString() {
        return getRole().map(r -> r.getPrintableName() + ": ").orElse("") + getRolePlayer().getPrintableName();
    }
}
