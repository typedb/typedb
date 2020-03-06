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
import graql.lang.property.DataTypeProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

/**
 * Atomic corresponding to DataTypeProperty.
 */
public class DataTypeAtom extends AtomicBase {

    private final AttributeType.DataType<?> dataType;

    private DataTypeAtom(Variable varName, Statement pattern, ReasonerQuery parentQuery, AttributeType.DataType<?> dataType) {
        super(parentQuery, varName, pattern);
        this.dataType = dataType;
    }

    public static DataTypeAtom create(Variable var, DataTypeProperty prop, ReasonerQuery parent, AttributeType.DataType<?> dataType) {
        Variable varName = var.asReturnedVar();
        return new DataTypeAtom(varName, new Statement(varName).datatype(prop.dataType()), parent, dataType);
    }

    private static DataTypeAtom create(DataTypeAtom a, ReasonerQuery parent) {
        return new DataTypeAtom(a.getVarName(), a.getPattern(), parent, a.getDataType());
    }

    public AttributeType.DataType<?> getDataType() {
        return dataType;
    }

    @Override
    public Atomic copy(ReasonerQuery parent) { return create(this, parent);}

    @Override
    public boolean isAlphaEquivalent(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        DataTypeAtom a2 = (DataTypeAtom) obj;
        return this.getDataType().equals(a2.getDataType());
    }

    @Override
    public int alphaEquivalenceHashCode() {
        return getDataType().hashCode();
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
