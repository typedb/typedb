/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.graql.internal.reasoner.atom.property;

import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.internal.reasoner.atom.AtomicBase;
import com.google.auto.value.AutoValue;

/**
 *
 * <p>
 * Atomic corresponding to {@link ai.grakn.graql.internal.pattern.property.IsAbstractProperty}.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
@AutoValue
public abstract class IsAbstractAtom extends AtomicBase {

    public static IsAbstractAtom create(Var varName, ReasonerQuery parent) {
        IsAbstractAtom atom = new AutoValue_IsAbstractAtom(varName, varName.isAbstract().admin());
        atom.parent = parent;
        return atom;
    }

    private static IsAbstractAtom create(IsAbstractAtom a, ReasonerQuery parent) {
        IsAbstractAtom atom = new AutoValue_IsAbstractAtom(a.getVarName(), a.getPattern());
        atom.parent = parent;
        return atom;
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

}
