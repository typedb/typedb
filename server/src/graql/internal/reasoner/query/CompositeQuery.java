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

package grakn.core.graql.internal.reasoner.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.MultiUnifier;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.admin.Unifier;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.Type;
import grakn.core.graql.internal.reasoner.ResolutionIterator;
import grakn.core.graql.internal.reasoner.cache.MultilevelSemanticCache;
import grakn.core.graql.internal.reasoner.state.ConjunctiveState;
import grakn.core.graql.internal.reasoner.state.CompositeState;
import grakn.core.graql.internal.reasoner.state.QueryStateBase;
import grakn.core.graql.internal.reasoner.state.ResolutionState;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Negation;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.statement.Statement;
import grakn.core.graql.query.pattern.statement.Variable;
import grakn.core.server.session.TransactionOLTP;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * A class representing a composite query: a conjunctive query containing a positive and negative part:
 *
 * For a conjunctive query Q := P, ¬R1, ¬R2, ... ¬Ri
 *
 * the corresponding composite query is:
 *
 * CQ : [ P, {R1, R2, ... Ri} ]
 *
 * The positive part P is defined by a conjunctive query.
 * The negative {R1, R2, ... Ri} part is a set of composite queries (we allow nesting).
 *
 * The negative part is stored in terms of the negation complement - hence all stored queries are positive.
 *
 */
public class CompositeQuery implements ReasonerQuery {

    final private ReasonerQueryImpl conjunctiveQuery;
    final private Set<CompositeQuery> complementQueries;
    final private TransactionOLTP tx;

    CompositeQuery(Conjunction<Pattern> pattern, TransactionOLTP tx) {
        Conjunction<Statement> positiveConj = Graql.and(
                pattern.getPatterns().stream()
                        .filter(p -> !p.isNegation())
                        .flatMap(p -> p.statements().stream())
                        .collect(Collectors.toSet())
        );
        this.tx = tx;
        //conjunction of negation patterns
        Set<Conjunction<Pattern>> complementPattern = complementPattern(pattern);
        this.conjunctiveQuery = new ReasonerQueryImpl(positiveConj, tx);
        this.complementQueries = complementPattern.stream()
                        .map(comp -> new CompositeQuery(comp, tx))
                        .collect(Collectors.toSet());

        if (!isNegationSafe()){
            throw new IllegalStateException("Query:\n" + this + "\nis unsafe! Negated pattern variables not bound!");
        }
    }

    CompositeQuery(ReasonerQueryImpl conj, Set<CompositeQuery> neg, TransactionOLTP tx) {
        this.conjunctiveQuery = conj;
        this.complementQueries = neg;
        this.tx = tx;
    }

    private boolean isNegationSafe(){
        return getComplementQueries().isEmpty() ||
                !Sets.intersection(getConjunctiveQuery().getVarNames(), getComplementQueries().stream().flatMap(q -> q.getVarNames().stream()).collect(Collectors.toSet())).isEmpty();
    }

    private Set<Conjunction<Pattern>> complementPattern(Conjunction<Pattern> pattern){
        return pattern.getPatterns().stream()
                .filter(Pattern::isNegation)
                .map(Pattern::asNegation)
                .map(Negation::getPattern)
                .map(p -> p.getNegationDNF().getPatterns().iterator().next())
                .collect(Collectors.toSet());
    }

    /**
     * @param sub substitution to be inserted into the query
     * @return corresponding query with additional substitution
     */
    CompositeQuery withSubstitution(ConceptMap sub){
        return new CompositeQuery(
                getConjunctiveQuery().withSubstitution(sub),
                getComplementQueries().stream().map(q -> q.withSubstitution(sub)).collect(Collectors.toSet()),
                tx()
        );
    }

    public CompositeQuery inferTypes() {
        return new CompositeQuery(getConjunctiveQuery().inferTypes(), getComplementQueries(), this.tx());
    }

    public ReasonerQueryImpl getConjunctiveQuery() {
        return conjunctiveQuery;
    }

    public Set<CompositeQuery> getComplementQueries() {
        return complementQueries;
    }

    @Override
    public boolean isPositive(){
        return complementQueries.isEmpty();
    }

    @Override
    public String toString(){
        return getConjunctiveQuery().toString() +
                (getComplementQueries() != null? "\nNOT{\n" + getComplementQueries() + "\n}" : "");
    }

    @Override
    public ReasonerQuery conjunction(ReasonerQuery q) {
        return new CompositeQuery(
                Graql.and(
                    Sets.union(
                        this.getPattern().getPatterns(),
                        q.getPattern().getPatterns()
                    )),
                this.tx()
        );
    }

    @Override
    public TransactionOLTP tx() { return tx; }

    @Override
    public void checkValid() {
        getConjunctiveQuery().checkValid();
        getComplementQueries().forEach(CompositeQuery::checkValid);
    }

    @Override
    public Set<Variable> getVarNames() {
        Set<Variable> varNames = getConjunctiveQuery().getVarNames();
        getComplementQueries().stream().flatMap(q -> q.getVarNames().stream()).forEach(varNames::add);
        return varNames;
    }

    @Override
    public Set<Atomic> getAtoms() {
        Set<Atomic> atoms = new HashSet<>(getConjunctiveQuery().getAtoms());
        getComplementQueries().stream().flatMap(q -> q.getAtoms().stream()).forEach(atoms::add);
        return atoms;
    }

    @Override
    public Conjunction<Pattern> getPattern() {
        Set<Pattern> pattern = Sets.newHashSet(getConjunctiveQuery().getPattern());
        getComplementQueries().stream().map(CompositeQuery::getPattern).forEach(pattern::add);
        return Graql.and(pattern);
    }

    @Override
    public ConceptMap getSubstitution() {
        ConceptMap sub = getConjunctiveQuery().getSubstitution();
        for (CompositeQuery comp : getComplementQueries()) {
            sub = sub.merge(comp.getSubstitution());
        }
        return sub;
    }

    @Override
    public Set<String> validateOntologically() {
        Set<String> validation = getConjunctiveQuery().validateOntologically();
        getComplementQueries().stream().map(CompositeQuery::validateOntologically).forEach(validation::addAll);
        return validation;
    }

    @Override
    public boolean isRuleResolvable() {
        return getConjunctiveQuery().isRuleResolvable() || getComplementQueries().stream().anyMatch(CompositeQuery::isRuleResolvable);
    }

    @Override
    public boolean isTypeRoleCompatible(Variable typedVar, Type parentType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MultiUnifier getMultiUnifier(ReasonerQuery parent) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ImmutableMap<Variable, Type> getVarTypeMap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ImmutableMap<Variable, Type> getVarTypeMap(boolean inferTypes) { return getVarTypeMap(); }

    @Override
    public ImmutableMap<Variable, Type> getVarTypeMap(ConceptMap sub) { return getVarTypeMap(); }

    @Override
    public Stream<ConceptMap> resolve() {
        return resolve(new MultilevelSemanticCache());
    }

    @Override
    public boolean requiresReiteration() {
        return getConjunctiveQuery().requiresReiteration() || getComplementQueries().stream().anyMatch(CompositeQuery::requiresReiteration);
    }

    public Stream<ConceptMap> resolve(MultilevelSemanticCache cache){
        return new ResolutionIterator(this, cache).hasStream();
    }

    /**
     * @param sub partial substitution
     * @param u unifier with parent state
     * @param parent parent state
     * @param subGoals set of visited sub goals
     * @param cache query cache
     * @return resolution subGoal formed from this query
     */
    public ResolutionState subGoal(ConceptMap sub, Unifier u, QueryStateBase parent, Set<ReasonerAtomicQuery> subGoals, MultilevelSemanticCache cache){
        return isPositive()?
                new ConjunctiveState(conjunctiveQuery, sub, u, parent, subGoals, cache) :
                new CompositeState(this, sub, u, parent, subGoals, cache);
    }
}
