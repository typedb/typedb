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
import grakn.core.common.exception.ErrorMessage;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.Rule;
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
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Negation;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

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
     * Vp = {xi, ..., xj}
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
     * Then, for the negation to be safe, we require:
     *
     * - the set of variables Vr to be non-empty
     *
     * NB: We do not require the negation blocks to be ground with respect to the positive part as we can rewrite the
     * negation blocks in terms of a ground relation defined via rule. i.e.:
     *
     * Q(x) :- T(x), (x, y), NOT{ (y, z) }
     *
     * can be rewritten as:
     *
     * Q(x) :- T(x), (x, y), NOT{ ?(y) }
     * ?(y) :- (y, z)
     *
     * @return true if this composite query is safe to resolve
     */
    private boolean isNegationSafe(){
        if (this.isPositive()) return true;
        if (bindingVariables().isEmpty()) return false;
        //check nested blocks
        return getComplementQueries().stream()
                .map(ResolvableQuery::asComposite)
                .allMatch(CompositeQuery::isNegationSafe);
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

    /**
     * Validate as rule body, Ensure:
     * - no negation nesting
     * - no disjunctions
     * - at most single negation block
     * @param graph transaction to be validated against
     * @param pattern pattern to be validated
     * @return set of error messages applicable
     */
    public static Set<String> validateAsRuleBody(Conjunction<Pattern> pattern, Rule rule, TransactionOLTP graph){
        Set<String> errors = new HashSet<>();
        try{
            CompositeQuery body = ReasonerQueries.composite(pattern, graph);
            Set<ResolvableQuery> complementQueries = body.getComplementQueries();
            if(complementQueries.size() > 1){
                errors.add(ErrorMessage.VALIDATION_RULE_MULTIPLE_NEGATION_BLOCKS.getMessage(rule.label()));
            }
            if(!body.isPositive() && complementQueries.stream().noneMatch(ReasonerQuery::isPositive)){
                errors.add(ErrorMessage.VALIDATION_RULE_NESTED_NEGATION.getMessage(rule.label()));
            }
        } catch (GraqlQueryException e) {
            errors.add(ErrorMessage.VALIDATION_RULE_INVALID.getMessage(rule.label(), e.getMessage()));
        }
        return errors;
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
    public ResolvableQuery neqPositive() {
        return new CompositeQuery(
                getConjunctiveQuery().neqPositive(),
                getComplementQueries(),
                tx());
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
