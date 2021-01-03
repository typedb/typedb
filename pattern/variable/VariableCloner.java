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
 *
 */

package grakn.core.pattern.variable;

import grakn.core.common.exception.GraknException;
import grakn.core.pattern.Conjunction;
import grakn.core.traversal.common.Identifier;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class VariableCloner {

    private final Map<Identifier.Variable, Variable> variables;

    public VariableCloner() {
        variables = new HashMap<>();
    }

    public static VariableCloner cloneFromConjunction(Conjunction conjunction) {
        VariableCloner cloner = new VariableCloner();
        conjunction.variables().forEach(cloner::clone);
        return cloner;
    }

    public Variable clone(Variable variable) {
        if (variable.isThing()) return clone(variable.asThing());
        else if (variable.isType()) return clone(variable.asType());
        else throw GraknException.of(ILLEGAL_STATE);
    }

    public ThingVariable clone(ThingVariable variable) {
        assert variable.identifier().isVariable();
        return variables.computeIfAbsent(variable.identifier().asVariable(), identifier -> {
            ThingVariable newClone = new ThingVariable(identifier);
            newClone.constrainClone(variable, this);
            return newClone;
        }).asThing();
    }

    public TypeVariable clone(TypeVariable variable) {
        assert variable.identifier().isVariable();
        return variables.computeIfAbsent(variable.identifier().asVariable(), identifier -> {
            TypeVariable newClone = new TypeVariable(identifier);
            newClone.constrainClone(variable, this);
            return newClone;
        }).asType();
    }

    public Set<Variable> variables() {
        return set(variables.values());
    }
}
