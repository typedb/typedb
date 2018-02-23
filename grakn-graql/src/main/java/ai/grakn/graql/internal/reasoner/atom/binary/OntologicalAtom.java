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

import ai.grakn.concept.Rule;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Base class for defining ontological {@link Atom} - ones referring to ontological elements.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class OntologicalAtom extends TypeAtom {

    @Override
    public boolean isSelectable() {
        return true;
    }

    @Override
    public Stream<InferenceRule> getApplicableRules() { return Stream.empty();}

    @Override
    public Set<String> validateAsRuleHead(Rule rule) {
        return Sets.newHashSet(ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD.getMessage(rule.getThen(), rule.getLabel()));
    }

    @Override
    public Set<TypeAtom> unify(Unifier u){
        Collection<Var> vars = u.get(getVarName());
        return vars.isEmpty()?
                Collections.singleton(this) :
                vars.stream().map(v -> AtomicFactory.createOntologicalAtom(this.getClass(), v, getPredicateVariable(), getTypePredicate(), this.getParentQuery())).collect(Collectors.toSet());
    }

    @Override
    public Atom rewriteWithTypeVariable() {
        return AtomicFactory.createOntologicalAtom(this.getClass(), getVarName(), getPredicateVariable().asUserDefined(), getTypePredicate(), getParentQuery());
    }

    @Override
    public Atom rewriteToUserDefined(Atom parentAtom) {
        return parentAtom.getPredicateVariable().isUserDefinedName()?
                AtomicFactory.createOntologicalAtom(this.getClass(), getVarName(), getPredicateVariable().asUserDefined(), getTypePredicate(), getParentQuery()) :
                this;
    }
}
