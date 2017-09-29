/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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
import ai.grakn.graql.internal.pattern.property.RegexProperty;
import ai.grakn.graql.internal.reasoner.atom.AtomicBase;

/**
 *
 * <p>
 * Atomic corresponding to {@link RegexProperty}.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class RegexAtom extends AtomicBase {

    private final String regex;

    public RegexAtom(Var varName, RegexProperty prop, ReasonerQuery parent){
        super(varName.regex(prop.regex()).admin(), parent);
        this.regex = prop.regex();
    }

    private RegexAtom(RegexAtom a) {
        super(a);
        this.regex = a.getRegex();
    }

    @Override
    public boolean equals(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        RegexAtom a2 = (RegexAtom) obj;
        return this.getRegex().equals(a2.getRegex()) &&
                this.getRegex().equals(((RegexAtom)obj).getRegex());
    }

    @Override
    public int hashCode(){
        int hashCode = alphaEquivalenceHashCode();
        hashCode = hashCode * 37 + this.getVarName().hashCode();
        return hashCode;
    }

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
        hashCode = hashCode * 37 + this.regex.hashCode();
        return hashCode;
    }

    @Override
    public boolean isStructurallyEquivalent(Object obj) {
        return isAlphaEquivalent(obj);
    }

    @Override
    public int structuralEquivalenceHashCode() { return alphaEquivalenceHashCode();}

    @Override
    public Atomic copy() { return new RegexAtom(this);}
    public String getRegex(){ return regex;}
}
