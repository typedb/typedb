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
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.Type;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.reasoner.ResolutionIterator;
import grakn.core.graql.internal.reasoner.atom.Atom;
import grakn.core.graql.internal.reasoner.atom.Atomic;
import grakn.core.graql.internal.reasoner.cache.MultilevelSemanticCache;
import grakn.core.graql.internal.reasoner.state.CompositeState;
import grakn.core.graql.internal.reasoner.state.QueryStateBase;
import grakn.core.graql.internal.reasoner.state.ResolutionState;
import grakn.core.graql.internal.reasoner.unifier.MultiUnifier;
import grakn.core.graql.internal.reasoner.unifier.Unifier;
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
public class CompositeQuery implements ResolvableQuery {

    final private ReasonerQueryImpl conjunctiveQuery;
    final private Set<ResolvableQuery> complementQueries;
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
        this.conjunctiveQuery = ReasonerQueries.create(positiveConj, tx);
        this.complementQueries = complementPattern.stream()
                .map(comp -> ReasonerQueries.resolvable(comp, tx))
                .collect(Collectors.toSet());

        if (!isNegationSafe()){
            throw GraqlQueryException.unsafeNegationBlock(this);
        }
    }

    CompositeQuery(ReasonerQueryImpl conj, Set<ResolvableQuery> comp, TransactionOLTP tx) {
        this.conjunctiveQuery = conj;
        this.complementQueries = comp;
        this.tx = tx;
    }

    @Override
    public CompositeQuery asComposite() {
        return this;
    }

    /**
     * We interpret negation blocks as equivalent to defining a rule with the content of the block being the rule body.
     * Writing the query in terms of variables it depends on we have:
     *
     * Q(x1, ..., xn) :- P1(xi, ...), ..., Pn(..., xj), NOT { R1(xk, ...), ..., Rn(..., xm) }
     *
     * We can then rewrite the negative part in terms of some unknown relation:
     *
     * ?(xk', ..., xm') :- R1(xk, ...), ..., Rn(..., xm)
     *
     * Where the sets of variables:
     * V = {x1, ..., xn}
     * Vp = {xi, ..., xj
     * Vn = {xk, ..., xm}
     * Vr = {xk', ..., xm'}
     *
     * satisfy:
     *
     * Vp e V
     * Vn e V
     * Vr e Vn
     *
     * This procedure can follow recursively for multiple nested negation blocks.
     * Then, for the negation to be safe, we require the variables of the head of the rules to be bound.
     * NB: as Vr is a subset of Vn, we do only require a subset of variables in the negation block to be bound.
     *
     * @return true if this composite query is safe to resolve
     */
    private boolean isNegationSafe(){
        if (isPositive()) return true;
        //check if p+1 is positive
        if (getComplementQueries().stream().allMatch(ReasonerQuery::isPositive)){
            //simple p boundedness check
            Set<Variable> intersection = Sets.intersection(
                    getConjunctiveQuery().getVarNames(),
                    getComplementQueries().stream().flatMap(q -> q.getVarNames().stream()).collect(Collectors.toSet())
            );
            return !intersection.isEmpty();
        } else {
            Set<CompositeQuery> p1Queries = getComplementQueries().stream().map(q -> (CompositeQuery) q).collect(Collectors.toSet());
            Set<ResolvableQuery> p2Queries = p1Queries.stream().flatMap(q -> q.getComplementQueries().stream()).collect(Collectors.toSet());
            //check if p+2 composite
            if (p2Queries.stream().noneMatch(ReasonerQuery::isPositive)){
                //do complex check
                Set<Variable> p0bindings = this.bindingVariables();
                Set<Set<Variable>> p1PositiveBindings = p1Queries.stream().map(p1q -> p1q.getConjunctiveQuery().getVarNames()).collect(Collectors.toSet());
                Set<Set<Variable>> p2PositiveBindings = p2Queries.stream().map(q -> (CompositeQuery) q).map(p2q -> p2q.getConjunctiveQuery().getVarNames()).collect(Collectors.toSet());
                if( !p1PositiveBindings.stream().allMatch(p1 -> p1.containsAll(p0bindings))
                    || !p2PositiveBindings.stream().allMatch(p2 -> p1PositiveBindings.stream().allMatch(p1 -> p1.containsAll(p2)))){
                    return false;
                }
            }
            return p1Queries.stream().allMatch(CompositeQuery::isNegationSafe);
        }
    }

    private Set<Variable> bindingVariables(){
        return Sets.intersection(getConjunctiveQuery().getVarNames(), getComplementQueries().stream().flatMap(q -> q.getVarNames().stream()).collect(Collectors.toSet()));
    }

    private Set<Conjunction<Pattern>> complementPattern(Conjunction<Pattern> pattern){
        return pattern.getPatterns().stream()
                .filter(Pattern::isNegation)
                .map(Pattern::asNegation)
                .map(Negation::getPattern)
                .map(p -> {
                    Set<Conjunction<Pattern>> patterns = p.getNegationDNF().getPatterns();
                    if (p.getNegationDNF().getPatterns().size() != 1){
                        throw GraqlQueryException.disjunctiveNegationBlock();
                    }
                    return Iterables.getOnlyElement(patterns);
                })
                .collect(Collectors.toSet());
    }

    @Override
    public CompositeQuery withSubstitution(ConceptMap sub){
        return new CompositeQuery(
                getConjunctiveQuery().withSubstitution(sub),
                getComplementQueries().stream().map(q -> q.withSubstitution(sub)).collect(Collectors.toSet()),
                tx()
        );
    }

    @Override
    public CompositeQuery inferTypes() {
        return new CompositeQuery(getConjunctiveQuery().inferTypes(), getComplementQueries(), this.tx());
    }

    @Override
    public ResolvableQuery positive() {
        return new CompositeQuery(getConjunctiveQuery().positive(), getComplementQueries(), tx());
    }

    public ReasonerQueryImpl getConjunctiveQuery() {
        return conjunctiveQuery;
    }

    public Set<ResolvableQuery> getComplementQueries() {
        return complementQueries;
    }

    @Override
    public ResolvableQuery copy() {
        return new CompositeQuery(
                getConjunctiveQuery().copy(),
                getComplementQueries().stream().map(ResolvableQuery::copy).collect(Collectors.toSet()),
                this.tx()
        );
    }

    @Override
    public boolean isAtomic() {
        return getComplementQueries().isEmpty() && getConjunctiveQuery().isAtomic();
    }

    @Override
    public boolean isComposite() { return true; }

    @Override
    public boolean isPositive(){
        return complementQueries.isEmpty();
    }

    @Override
    public boolean isEquivalent(ResolvableQuery q) {
        CompositeQuery that = q.asComposite();
        return getConjunctiveQuery().isEquivalent(that.getConjunctiveQuery())
                && getComplementQueries().size() == that.getComplementQueries().size()
                && getComplementQueries().stream().allMatch(c -> that.getComplementQueries().stream().anyMatch(c::isEquivalent));
    }

    @Override
    public String toString(){
        String complementString = getComplementQueries().stream()
                .map(q -> "\nNOT {" + q.toString() + "\n}")
                .collect(Collectors.joining());
        return getConjunctiveQuery().toString() +
                (!getComplementQueries().isEmpty()? complementString : "");
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
        getComplementQueries().forEach(ResolvableQuery::checkValid);
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
        getComplementQueries().stream().map(ResolvableQuery::getPattern).forEach(pattern::add);
        return Graql.and(pattern);
    }

    @Override
    public ConceptMap getSubstitution() {
        ConceptMap sub = getConjunctiveQuery().getSubstitution();
        for (ResolvableQuery comp : getComplementQueries()) {
            sub = sub.merge(comp.getSubstitution());
        }
        return sub;
    }

    @Override
    public Set<String> validateOntologically() {
        Set<String> validation = getConjunctiveQuery().validateOntologically();
        getComplementQueries().stream().map(ResolvableQuery::validateOntologically).forEach(validation::addAll);
        return validation;
    }

    @Override
    public boolean isRuleResolvable() {
        return getConjunctiveQuery().isRuleResolvable() || getComplementQueries().stream().anyMatch(ResolvableQuery::isRuleResolvable);
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
        return resolve(new HashSet<>(), new MultilevelSemanticCache(), this.requiresReiteration());
    }

    @Override
    public Stream<ConceptMap> resolve(Set<ReasonerAtomicQuery> subGoals, MultilevelSemanticCache cache, boolean reiterate){
        return new ResolutionIterator(this, subGoals, cache, reiterate).hasStream();
    }

    @Override
    public boolean requiresReiteration() {
        return getConjunctiveQuery().requiresReiteration() || getComplementQueries().stream().anyMatch(ResolvableQuery::requiresReiteration);
    }

    @Override
    public Stream<Atom> selectAtoms() {
        return getAtoms(Atom.class).filter(Atomic::isSelectable);
    }

    @Override
    public boolean requiresDecomposition() {
        return getConjunctiveQuery().requiresDecomposition() ||
                (!getComplementQueries().isEmpty() && getComplementQueries().stream().anyMatch(ResolvableQuery::requiresDecomposition));
    }

    @Override
    public CompositeQuery rewrite(){
        return new CompositeQuery(
                getConjunctiveQuery().rewrite(),
                getComplementQueries().isEmpty()?
                        getComplementQueries() :
                        getComplementQueries().stream().map(ResolvableQuery::rewrite).collect(Collectors.toSet()),
                tx()
        );
    }

    @Override
    public ResolutionState subGoal(ConceptMap sub, Unifier u, QueryStateBase parent, Set<ReasonerAtomicQuery> subGoals, MultilevelSemanticCache cache){
        return isPositive()?
                getConjunctiveQuery().subGoal(sub, u, parent, subGoals, cache) :
                new CompositeState(this, sub, u, parent, subGoals, cache);
    }
}
