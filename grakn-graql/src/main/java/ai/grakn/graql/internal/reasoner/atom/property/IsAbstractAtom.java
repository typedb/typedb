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

import ai.grakn.graql.Graql;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.internal.reasoner.atom.AtomBase;

/**
 *
 * <p>
 * Atomic corresponding to graql IsAbstractProperty.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class IsAbstractAtom extends AtomBase {

    public IsAbstractAtom(VarName varName, ReasonerQuery parent){
        super(Graql.var(varName).isAbstract().admin(), parent);
    }

    private IsAbstractAtom(IsAbstractAtom a){ super(a);}

    @Override
    public boolean equals(Object obj){
        return !(obj == null || this.getClass() != obj.getClass());
    }

    @Override
    public int hashCode(){
        int hashCode = equivalenceHashCode();
        hashCode = hashCode * 37 + this.varName.hashCode();
        return hashCode;
    }

    @Override
    public boolean isEquivalent(Object obj) {
        return !(obj == null || this.getClass() != obj.getClass());
    }

    @Override
    public int equivalenceHashCode() { return 1;}

    @Override
    public Atomic copy() { return new IsAbstractAtom(this); }

}
