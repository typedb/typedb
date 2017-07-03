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

package ai.grakn.graql.internal.reasoner.atom.binary.type;

import ai.grakn.graql.Graql;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.pattern.property.HasResourceTypeProperty;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import java.util.Set;

public class HasResourceTypeAtom extends TypeAtom {

    public HasResourceTypeAtom(VarPatternAdmin pattern, ReasonerQuery par) { super(pattern, Graql.var().asUserDefined(), null, par);}
    private HasResourceTypeAtom(TypeAtom a) { super(a);}

    @Override
    public Atomic copy(){
        return new HasResourceTypeAtom(this);
    }

    @Override
    public VarProperty getVarProperty() {
        return getPattern().asVar().getProperties().filter(p -> p instanceof HasResourceTypeProperty).findFirst().orElse(null);
    }

    @Override
    public Set<TypeAtom> unify(Unifier u) {
        return null;
    }
}
