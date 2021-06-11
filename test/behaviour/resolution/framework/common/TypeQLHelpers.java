/*
 * Copyright (C) 2021 Vaticle
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
 *
 */

package com.vaticle.typedb.core.test.behaviour.resolution.framework.common;

import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.constraint.Constraint;
import com.vaticle.typeql.lang.pattern.variable.BoundVariable;
import com.vaticle.typeql.lang.pattern.variable.UnboundVariable;

import java.util.List;


public class TypeQLHelpers {

    public static BoundVariable makeAnonVarsExplicit(BoundVariable variable) {
        if (variable.isNamed()) {
            return variable;
        } else {
            List<? extends Constraint<?>> constraints = variable.constraints();
            // TODO: Generate some new name
            BoundVariable vb;
            UnboundVariable v = TypeQL.var("bob");
            // TODO: This needs to return a bound variable. The variables must have one or more constraints, but how can we know this?
            constraints.forEach(constraint -> {vb = v.constrain(constraint)});
            return vb;
        }
    }
}
