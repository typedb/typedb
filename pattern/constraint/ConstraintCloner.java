/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.pattern.constraint;

import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.Variable;
import grakn.core.traversal.common.Identifier;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static grakn.common.collection.Collections.set;

public class ConstraintCloner {

    private final Map<Identifier.Variable, Variable> variables;
    private final Map<Constraint, Constraint> constraints;

    public ConstraintCloner() {
        variables = new HashMap<>();
        constraints = new HashMap<>();
    }

    public static ConstraintCloner cloneFromConstraints(Set<Constraint> constraints) {
        ConstraintCloner cloner = new ConstraintCloner();
        constraints.forEach(cloner::clone);
        return cloner;
    }

    private void clone(Constraint constraint) {
        constraints.put(constraint, constraint.clone(this));
    }

    public ThingVariable cloneVariable(ThingVariable variable) {
        return variables.computeIfAbsent(variable.id(), identifier -> {
            ThingVariable clone = new ThingVariable(identifier);
            clone.addResolvedTypes(variable.resolvedTypes());
            return clone;
        }).asThing();
    }

    public TypeVariable cloneVariable(TypeVariable variable) {
        return variables.computeIfAbsent(variable.id(), identifier -> {
            TypeVariable clone = new TypeVariable(identifier);
            clone.addResolvedTypes(variable.resolvedTypes());
            return clone;
        }).asType();
    }

    public Set<Variable> variables() {
        return set(variables.values());
    }

    public Constraint getClone(Constraint constraint) {
        return constraints.get(constraint);
    }
}
