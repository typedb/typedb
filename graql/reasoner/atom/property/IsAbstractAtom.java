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
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

/**
 * Atomic corresponding to AbstractProperty.
 */
public class IsAbstractAtom extends AtomicBase {

    private IsAbstractAtom(Variable varName, Statement pattern, ReasonerQuery reasonerQuery) {
        super(reasonerQuery, varName, pattern);
    }

    public static IsAbstractAtom create(Variable var, ReasonerQuery parent) {
        Variable varName = var.asReturnedVar();
        return new IsAbstractAtom(varName, new Statement(varName).isAbstract(), parent);
    }

    private static IsAbstractAtom create(IsAbstractAtom a, ReasonerQuery parent) {
        return new IsAbstractAtom(a.getVarName(), a.getPattern(), parent);
    }

    @Override
    public Atomic copy(ReasonerQuery parent) { return create(this, parent); }

    @Override
    public boolean isAlphaEquivalent(Object obj) {
        return !(obj == null || this.getClass() != obj.getClass());
    }

    @Override
    public int alphaEquivalenceHashCode() { return 1;}

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
