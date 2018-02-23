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

package ai.grakn.graql.internal.reasoner.atom.binary;

import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.pattern.property.HasAttributeTypeProperty;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import com.google.auto.value.AutoValue;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * TypeAtom corresponding to graql a {@link HasAttributeTypeProperty} property.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
@AutoValue
public abstract class HasAtom extends OntologicalAtom {

    public static HasAtom create(VarPattern pattern, Var predicateVar, IdPredicate p, ReasonerQuery parent) {
        HasAtom atom = new AutoValue_HasAtom(pattern.admin().var(), pattern, predicateVar, p);
        atom.parent = parent;
        return atom;
    }

    private static HasAtom create(Var var, Var predicateVar, IdPredicate p, ReasonerQuery parent) {
        return create(var.has(predicateVar), predicateVar, p, parent);
    }

    private static HasAtom create(TypeAtom a, ReasonerQuery parent) {
        return create(a.getPattern(), a.getPredicateVariable(), a.getTypePredicate(), parent);
    }

    @Override
    public Atomic copy(ReasonerQuery parent){ return create(this, parent); }

    @Override
    public Class<? extends VarProperty> getVarPropertyClass() { return HasAttributeTypeProperty.class;}

    @Override
    public Set<TypeAtom> unify(Unifier u){
        Collection<Var> vars = u.get(getVarName());
        return vars.isEmpty()?
                Collections.singleton(this) :
                vars.stream().map(v -> create(v, getPredicateVariable(), getTypePredicate(), this.getParentQuery())).collect(Collectors.toSet());
    }

    @Override
    public Atom rewriteWithTypeVariable() {
        return create(getPattern(), getPredicateVariable().asUserDefined(), getTypePredicate(), getParentQuery());
    }

    @Override
    public Atom rewriteToUserDefined(Atom parentAtom) {
        return parentAtom.getPredicateVariable().isUserDefinedName()?
                create(getPattern(), getPredicateVariable().asUserDefined(), getTypePredicate(), getParentQuery()) :
                this;
    }
}
