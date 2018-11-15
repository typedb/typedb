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

package grakn.core.graql.internal.reasoner.atom;

import grakn.core.graql.concept.Rule;
import grakn.core.graql.Pattern;
import grakn.core.graql.Var;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.internal.query.answer.ConceptMapImpl;
import grakn.core.graql.internal.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.internal.reasoner.atom.predicate.Predicate;
import grakn.core.server.session.TransactionImpl;
import grakn.core.commons.exception.ErrorMessage;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Base {@link Atomic} implementation providing basic functionalities.
 * </p>
 *
 *
 */
public abstract class AtomicBase implements Atomic {

    @Override public void checkValid(){}

    @Override
    public Set<String> validateAsRuleHead(Rule rule) {
        return Sets.newHashSet(ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD.getMessage(rule.then(), rule.label()));
    }

    @Override
    public String toString(){ return getPattern().toString(); }

    boolean containsVar(Var name){ return getVarNames().contains(name);}

    public boolean isUserDefined(){ return getVarName().isUserDefinedName();}

    /**
     * @return set of predicates relevant to this atomic
     */
    public Stream<Predicate> getPredicates() {
        return getPredicates(Predicate.class);
    }

    /**
     * @param type the class of {@link Predicate} to return
     * @param <T> the type of {@link Predicate} to return
     * @return stream of predicates relevant to this atomic
     */
    public <T extends Predicate> Stream<T> getPredicates(Class<T> type) {
        return getParentQuery().getAtoms(type).filter(atom -> !Sets.intersection(this.getVarNames(), atom.getVarNames()).isEmpty());
    }

    /**
     * @param var variable the predicate refers to
     * @param type predicate type
     * @param <T> predicate type generic
     * @return specific predicates referring to provided variable
     */
    public <T extends Predicate> Stream<T> getPredicates(Var var, Class<T> type){
        return getPredicates(type).filter(p -> p.getVarName().equals(var));
    }

    /**
     * @param var variable of interest
     * @return id predicate referring to prescribed variable
     */
    @Nullable
    public IdPredicate getIdPredicate(Var var){
        return getPredicates(var, IdPredicate.class).findFirst().orElse(null);
    }

    @Override
    public Set<Var> getVarNames(){
        Var varName = getVarName();
        return varName.isUserDefinedName()? Sets.newHashSet(varName) : Collections.emptySet();
    }

    protected Pattern createCombinedPattern(){ return getPattern();}

    @Override
    public Pattern getCombinedPattern(){
        return createCombinedPattern();
    }

    @Override
    public Atomic inferTypes(){ return inferTypes(new ConceptMapImpl()); }

    public Atomic inferTypes(ConceptMap sub){ return this; }

    /**
     * @return Transaction this atomic is defined in
     */
    protected TransactionImpl<?> tx(){
        // TODO: This cast is unsafe - ReasonerQuery should return an TransactionImpl
        return (TransactionImpl<?>) getParentQuery().tx();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof AtomicBase) {
            AtomicBase that = (AtomicBase) o;
            return (this.getVarName().equals(that.getVarName()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.getVarName().hashCode();
        return h;
    }
}

