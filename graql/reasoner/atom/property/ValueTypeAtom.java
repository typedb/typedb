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

package grakn.core.graql.reasoner.atom.property;

import grakn.core.graql.reasoner.atom.AtomicBase;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import graql.lang.property.ValueTypeProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

/**
 * Atomic corresponding to ValueTypeProperty.
 */
public class ValueTypeAtom extends AtomicBase {

    private final AttributeType.ValueType<?> valueType;

    private ValueTypeAtom(Variable varName, Statement pattern, ReasonerQuery parentQuery, AttributeType.ValueType<?> valueType) {
        super(parentQuery, varName, pattern);
        this.valueType = valueType;
    }

    public static ValueTypeAtom create(Variable var, ValueTypeProperty prop, ReasonerQuery parent, AttributeType.ValueType<?> valueType) {
        Variable varName = var.asReturnedVar();
        return new ValueTypeAtom(varName, new Statement(varName).value(prop.valueType()), parent, valueType);
    }

    private static ValueTypeAtom create(ValueTypeAtom a, ReasonerQuery parent) {
        return new ValueTypeAtom(a.getVarName(), a.getPattern(), parent, a.getValueType());
    }

    public AttributeType.ValueType<?> getValueType() {
        return valueType;
    }

    @Override
    public Atomic copy(ReasonerQuery parent) { return create(this, parent);}

    @Override
    public boolean isAlphaEquivalent(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        ValueTypeAtom a2 = (ValueTypeAtom) obj;
        return this.getValueType().equals(a2.getValueType());
    }

    @Override
    public int alphaEquivalenceHashCode() {
        return getValueType().hashCode();
    }

    @Override
    public boolean isStructurallyEquivalent(Object obj) {
        return isAlphaEquivalent(obj);
    }

    @Override
    public int structuralEquivalenceHashCode() {
        return alphaEquivalenceHashCode();
    }

    @Override
    public boolean isSubsumedBy(Atomic atom) { return this.isAlphaEquivalent(atom); }
}
