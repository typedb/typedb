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

package ai.grakn.graql.internal.reasoner.atom.binary;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Rule;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.atom.Atom;
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

    abstract OntologicalAtom createSelf(Var var, Var predicateVar, ConceptId predicateId, ReasonerQuery parent);

    @Override
    public boolean isSelectable() {
        return true;
    }

    @Override
    public Stream<Rule> getPotentialRules(){ return Stream.empty();}

    @Override
    public Stream<InferenceRule> getApplicableRules() { return Stream.empty();}

    @Override
    public Set<String> validateAsRuleHead(Rule rule) {
        return Sets.newHashSet(ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD.getMessage(rule.then(), rule.label()));
    }

    @Override
    public Set<TypeAtom> unify(Unifier u){
        Collection<Var> vars = u.get(getVarName());
        return vars.isEmpty()?
                Collections.singleton(this) :
                vars.stream().map(v -> createSelf(v, getPredicateVariable(), getTypeId(), this.getParentQuery())).collect(Collectors.toSet());
    }

    @Override
    public Atom rewriteWithTypeVariable() {
        return createSelf(getVarName(), getPredicateVariable().asUserDefined(), getTypeId(), getParentQuery());
    }

    @Override
    public Atom rewriteToUserDefined(Atom parentAtom) {
        return parentAtom.getPredicateVariable().isUserDefinedName()?
                createSelf(getVarName(), getPredicateVariable().asUserDefined(), getTypeId(), getParentQuery()) :
                this;
    }

    @Override
    public final boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof OntologicalAtom) {
            OntologicalAtom that = (OntologicalAtom) o;
            return (this.getVarName().equals(that.getVarName()))
                    && ((this.getTypeId() == null) ? (that.getTypeId() == null) : this.getTypeId().equals(that.getTypeId()));
        }
        return false;
    }

    @Override
    public final int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.getVarName().hashCode();
        h *= 1000003;
        h ^= (getTypeId() == null) ? 0 : this.getTypeId().hashCode();
        return h;
    }
}
