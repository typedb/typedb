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
import graql.lang.property.RegexProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

/**
 * Atomic corresponding to RegexProperty.
 */
public class RegexAtom extends AtomicBase {

    private final String regex;

    private RegexAtom(Variable varName, Statement pattern, ReasonerQuery parent, String regex) {
        super(parent, varName, pattern);
        this.regex = regex;
    }

    public static RegexAtom create(Variable var, RegexProperty prop, ReasonerQuery parent) {
        Variable varName = var.asReturnedVar();
        return new RegexAtom(varName, new Statement(varName).regex(prop.regex()), parent, prop.regex());
    }

    private static RegexAtom create(RegexAtom a, ReasonerQuery parent) {
        return new RegexAtom(a.getVarName(), a.getPattern(), parent, a.getRegex());
    }

    public String getRegex() {
        return regex;
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
        return getRegex().hashCode();
    }

    @Override
    public boolean isStructurallyEquivalent(Object obj) {
        return isAlphaEquivalent(obj);
    }

    @Override
    public int structuralEquivalenceHashCode() { return alphaEquivalenceHashCode();}

    @Override
    public boolean isSubsumedBy(Atomic atom) { return this.isAlphaEquivalent(atom); }
}
