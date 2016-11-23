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
package ai.grakn.graql.internal.reasoner.atom;

import ai.grakn.concept.Type;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.reasoner.query.Query;
import java.util.Set;

public class TypeAtom extends Binary{

    public TypeAtom(VarAdmin pattern) { this(pattern, null);}
    public TypeAtom(VarAdmin pattern, Query par) {
        this(pattern, null, par);
    }
    public TypeAtom(VarAdmin pattern, Predicate p, Query par) { super(pattern, p, par);}
    protected TypeAtom(TypeAtom a) { super(a);}

    @Override
    protected String extractTypeId(VarAdmin var) {
        return getPredicate() != null? getPredicate().getPredicateValue() : "";
    }

    @Override
    protected String extractValueVariableName(VarAdmin var) {
        return var.getProperties().findFirst().orElse(null).getTypes().findFirst().orElse(null).getVarName();
    }

    @Override
    protected void setValueVariable(String var) {
        super.setValueVariable(var);
        atomPattern.asVar().getProperties(IsaProperty.class).forEach(prop -> prop.getType().setName(var));
    }

    @Override
    public Atomic clone(){
        return new TypeAtom(this);
    }

    @Override
    public boolean isType(){ return true;}

    @Override
    public Type getType() {
        return getPredicate() != null ?
                getParentQuery().getGraph().orElse(null).getType(getPredicate().getPredicateValue()) : null;
    }

    @Override
    public Set<Predicate> getIdPredicates() {
        Set<Predicate> idPredicates = super.getIdPredicates();
        if (getPredicate() != null) idPredicates.add(getPredicate());
        return idPredicates;
    }
}

