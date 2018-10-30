/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package ai.grakn.graql.internal.reasoner.atom.property;

import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.internal.pattern.property.RegexProperty;
import ai.grakn.graql.internal.reasoner.atom.AtomicBase;
import com.google.auto.value.AutoValue;

/**
 *
 * <p>
 * Atomic corresponding to {@link RegexProperty}.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
@AutoValue
public abstract class RegexAtom extends AtomicBase {

    @Override public abstract VarPattern getPattern();
    @Override public abstract ReasonerQuery getParentQuery();
    public abstract String getRegex();

    public static RegexAtom create(Var varName, RegexProperty prop, ReasonerQuery parent) {
        return new AutoValue_RegexAtom(varName, varName.regex(prop.regex()).admin(), parent, prop.regex());
    }

    private static RegexAtom create(RegexAtom a, ReasonerQuery parent) {
        return new AutoValue_RegexAtom(a.getVarName(), a.getPattern(), parent, a.getRegex());
    }

    @Override
    public Atomic copy(ReasonerQuery parent) { return create(this, parent);}

    @Override
    public boolean isAlphaEquivalent(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        RegexAtom a2 = (RegexAtom) obj;
        return this.getRegex().equals(a2.getRegex());
    }

    @Override
    public int alphaEquivalenceHashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.getRegex().hashCode();
        return hashCode;
    }

    @Override
    public boolean isStructurallyEquivalent(Object obj) {
        return isAlphaEquivalent(obj);
    }

    @Override
    public int structuralEquivalenceHashCode() { return alphaEquivalenceHashCode();}
}
