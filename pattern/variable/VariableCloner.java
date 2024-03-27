/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.pattern.variable;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

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
        else if (variable.isValue()) return clone(variable.asValue());
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    public ThingVariable clone(ThingVariable variable) {
        assert variable.id().isVariable();
        if (variables.containsKey(variable.id())) return variables.get(variable.id()).asThing();
        ThingVariable newClone = variable.clone();
        variables.put(variable.id(), newClone);
        newClone.constrainClone(variable, this);
        return newClone;
    }

    public TypeVariable clone(TypeVariable variable) {
        assert variable.id().isVariable();
        if (variables.containsKey(variable.id())) return variables.get(variable.id()).asType();
        TypeVariable newClone = variable.clone();
        variables.put(variable.id(), newClone);
        newClone.constrainClone(variable, this);
        return newClone;
    }

    public ValueVariable clone(ValueVariable variable) {
        assert variable.id().isVariable();
        if (variables.containsKey(variable.id())) return variables.get(variable.id()).asValue();
        ValueVariable newClone = variable.clone();
        variables.put(variable.id(), newClone);
        newClone.constrainClone(variable, this);
        return newClone;
    }

    public Set<Variable> variables() {
        return set(variables.values());
    }
}
