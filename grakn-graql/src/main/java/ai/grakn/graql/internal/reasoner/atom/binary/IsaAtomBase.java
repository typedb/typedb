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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.atom.binary;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.utils.Pair;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * //TODO
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class IsaAtomBase extends TypeAtom{

    Pair<VarPattern, IdPredicate> getTypedPair(SchemaConcept type){
        ConceptId typeId = type.getId();
        Var typeVariable = getPredicateVariable().getValue().isEmpty() ? Graql.var().asUserDefined() : getPredicateVariable();

        IdPredicate newPredicate = IdPredicate.create(typeVariable.id(typeId).admin(), getParentQuery());
        return new Pair<>(getPattern(), newPredicate);
    }

    @Override
    public Set<TypeAtom> unify(Unifier u){
        Collection<Var> vars = u.get(getVarName());
        return vars.isEmpty()?
                Collections.singleton(this) :
                vars.stream().map(v -> IsaAtom.create(v, getPredicateVariable(), getTypeId(), this.getParentQuery())).collect(Collectors.toSet());
    }
}
