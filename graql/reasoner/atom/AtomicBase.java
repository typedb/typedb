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

package grakn.core.graql.reasoner.atom;

import com.google.common.collect.Sets;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.graql.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.reasoner.atom.predicate.Predicate;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;

/**
 * Base Atomic implementation providing basic functionalities.
 */
public abstract class AtomicBase implements Atomic {

    private final ReasonerQuery parentQuery;
    private final Variable varName;
    private final Statement pattern;

    public AtomicBase(ReasonerQuery parentQuery, Variable varName, Statement pattern) {
        this.parentQuery = parentQuery;
        this.varName = varName;
        this.pattern = pattern;
    }

    @CheckReturnValue
    @Override
    public Variable getVarName() {
        return varName;
    }

    @CheckReturnValue
    @Override
    public Statement getPattern() {
        return pattern;
    }

    @Override
    public ReasonerQuery getParentQuery() {
        return parentQuery;
    }

    @Override public void checkValid(){}

    @Override
    public Set<String> validateAsRuleHead(Rule rule) {
        return Sets.newHashSet(ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD.getMessage(rule.then(), rule.label()));
    }

    @Override
    public Set<String> validateAsRuleBody(Label ruleLabel) {
        return new HashSet<>();
    }

    @Override
    public String toString(){ return getPattern().toString(); }

    boolean containsVar(Variable name){ return getVarNames().contains(name);}

    public boolean isUserDefined(){ return getVarName().isReturned();}

    /**
     * @return set of predicates relevant to this atomic
     */
    public Stream<Predicate> getPredicates() {
        return getPredicates(Predicate.class);
    }

    /**
     * @param type the class of Predicate to return
     * @param <T> the type of Predicate to return
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
    public <T extends Predicate> Stream<T> getPredicates(Variable var, Class<T> type){
        return getPredicates(type).filter(p -> p.getVarName().equals(var));
    }

    /**
     * @param var variable of interest
     * @return id predicate referring to prescribed variable
     */
    @Nullable
    public IdPredicate getIdPredicate(Variable var){
        return getPredicates(var, IdPredicate.class).findFirst().orElse(null);
    }

    @Override
    public Set<Variable> getVarNames(){
        Variable varName = getVarName();
        return varName.isReturned()? Sets.newHashSet(varName) : Collections.emptySet();
    }

    protected Pattern createCombinedPattern(){ return getPattern();}

    @Override
    public Pattern getCombinedPattern(){
        return createCombinedPattern();
    }

    @Override
    public Atomic inferTypes() { return this; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AtomicBase that = (AtomicBase) o;
        return Objects.equals(varName, that.varName);
    }

    @Override
    public int hashCode() {
        return varName.hashCode();
    }
}

