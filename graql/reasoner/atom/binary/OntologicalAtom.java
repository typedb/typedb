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

package grakn.core.graql.reasoner.atom.binary;

import com.google.common.collect.Sets;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.task.relate.BinarySemanticProcessor;
import grakn.core.graql.reasoner.rule.InferenceRule;
import grakn.core.graql.reasoner.unifier.MultiUnifierImpl;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.graql.reasoner.ReasonerException;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Base class for defining ontological Atom - ones referring to ontological elements.
 */
public abstract class OntologicalAtom extends TypeAtom {

    BinarySemanticProcessor semanticProcessor = new BinarySemanticProcessor();

    OntologicalAtom(Variable varName, Statement pattern, ReasonerQuery reasonerQuery, @Nullable Label label,
                    Variable predicateVariable, ReasoningContext ctx) {
        super(varName, pattern, reasonerQuery, label, predicateVariable, ctx);
    }

    abstract OntologicalAtom createSelf(Variable var, Variable predicateVar, @Nullable Label label, ReasonerQuery parent);

    @Override
    public String toString() {
        return getPattern().toString() +
                (getTypeLabel() != null ? getTypeLabel() : "");
    }

    @Override
    public boolean isSelectable() {
        return true;
    }

    @Override
    public boolean isSubsumedBy(Atomic atom) {
        return this.isAlphaEquivalent(atom);
    }

    @Override
    public Stream<Rule> getPotentialRules() {
        return Stream.empty();
    }

    @Override
    public Stream<InferenceRule> getApplicableRules() {
        return Stream.empty();
    }

    @Override
    public Set<String> validateAsRuleHead(Rule rule) {
        return Sets.newHashSet(ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD.getMessage(rule.then(), rule.label()));
    }

    @Override
    public Set<TypeAtom> unify(Unifier u) {
        Collection<Variable> vars = u.get(getVarName());
        return vars.isEmpty() ?
                Collections.singleton(this) :
                vars.stream().map(v -> createSelf(v, getPredicateVariable(), getTypeLabel(), this.getParentQuery())).collect(Collectors.toSet());
    }

    @Override
    public MultiUnifier getMultiUnifier(Atom parentAtom, UnifierType unifierType) {
        return semanticProcessor.getMultiUnifier(this, parentAtom, unifierType, context());
    }

    @Override
    public Atom rewriteWithTypeVariable() {
        return createSelf(getVarName(), getPredicateVariable().asReturnedVar(), getTypeLabel(), getParentQuery());
    }

    @Override
    public Atom rewriteToUserDefined(Atom parentAtom) {
        return parentAtom.getPredicateVariable().isReturned() ?
                createSelf(getVarName(), getPredicateVariable().asReturnedVar(), getTypeLabel(), getParentQuery()) :
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
                    && ((this.getTypeLabel() == null) ? (that.getTypeLabel() == null) : this.getTypeLabel().equals(that.getTypeLabel()));
        }
        return false;
    }

    @Override
    public final int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.getVarName().hashCode();
        h *= 1000003;
        h ^= (getTypeLabel() == null) ? 0 : this.getTypeLabel().hashCode();
        return h;
    }
}
