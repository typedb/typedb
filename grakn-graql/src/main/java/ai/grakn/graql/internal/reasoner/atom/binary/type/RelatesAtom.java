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

import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;


/**
 *
 * <p>
 * TypeAtom corresponding to a graql {@link ai.grakn.graql.internal.pattern.property.RelatesProperty} property.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class RelatesAtom extends TypeAtom {
    public RelatesAtom(VarPattern pattern, Var predicateVar, IdPredicate p, ReasonerQuery par) {
        super(pattern, predicateVar, p, par);}
    private RelatesAtom(Var var, Var predicateVar, IdPredicate p, ReasonerQuery par){
        this(var.relates(predicateVar), predicateVar, p, par);
    }
    private RelatesAtom(RelatesAtom a) { super(a);}

    @Override
    public Atomic copy(){
        return new RelatesAtom(this);
    }

    @Override
    public Set<TypeAtom> unify(Unifier u){
        Collection<Var> vars = u.get(getVarName());
        return vars.isEmpty()?
                Collections.singleton(this) :
                vars.stream().map(v -> new RelatesAtom(v, getPredicateVariable(), getTypePredicate(), this.getParentQuery())).collect(Collectors.toSet());
    }

    @Override
    public Atom rewriteWithTypeVariable() {
        return new RelatesAtom(getPattern(), getPredicateVariable().asUserDefined(), getTypePredicate(), getParentQuery());
    }

    @Override
    public Atom rewriteToUserDefined(Atom parentAtom) {
        return parentAtom.getPredicateVariable().isUserDefinedName()?
                new RelatesAtom(getPattern(), getPredicateVariable().asUserDefined(), getTypePredicate(), getParentQuery()) :
                this;
    }
}
