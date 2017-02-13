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

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.graql.Pattern;


/**
 * Represents the {@code rhs} (right-hand side) property on a {@link ai.grakn.concept.Rule}.
 *
 * This property can be inserted and not queried.
 *
 * The right-hand side describes the right-hand of an implication, stating that when the left-hand side of a rule is
 * true the right-hand side must hold.
 *
 * @author Felix Chapman
 */
public class RhsProperty extends RuleProperty {

    public RhsProperty(Pattern rhs) {
        super(rhs);
    }

    @Override
    public String getName() {
        return "rhs";
    }

}
