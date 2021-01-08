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

import grakn.core.pattern.Conjunction;
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

    public ConstraintCloner() {
        variables = new HashMap<>();
    }

    public static ConstraintCloner cloneFromConjunction(Set<Constraint> include) {
        ConstraintCloner cloner = new ConstraintCloner();
        include.forEach(cloner::clone);
        return cloner;
    }

    private void clone(Constraint constraint) {
        constraint.clone(this);
    }

    public ThingVariable cloneVariable(ThingVariable variable) {
        return variables.computeIfAbsent(variable.id().asVariable(), identifier -> {
            ThingVariable clone = new ThingVariable(identifier);
            clone.addResolvedTypes(variable.resolvedTypes());
            return clone;
        }).asThing();
    }

    public TypeVariable cloneVariable(TypeVariable variable) {
        return variables.computeIfAbsent(variable.id().asVariable(), identifier -> {
            TypeVariable clone = new TypeVariable(identifier);
            clone.addResolvedTypes(variable.resolvedTypes());
            return clone;
        }).asType();
    }

    public Set<Variable> variables() {
        return set(variables.values());
    }
}
