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

package grakn.core.graql.reasoner.query;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.ResolutionIterator;
import grakn.core.graql.reasoner.explanation.CompositeExplanation;
import grakn.core.graql.reasoner.explanation.LookupExplanation;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.graql.executor.TraversalExecutor;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.state.AnswerPropagatorState;
import grakn.core.graql.reasoner.state.CompositeState;
import grakn.core.graql.reasoner.state.ResolutionState;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.graql.reasoner.ReasonerException;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Negation;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
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
public class CompositeQuery extends ResolvableQuery {

    final private ReasonerQueryImpl conjunctiveQuery;
    final private Set<ResolvableQuery> complementQueries;
    private Conjunction<Pattern> pattern = null;

    CompositeQuery(Conjunction<Pattern> pattern, TraversalExecutor traversalExecutor, ReasoningContext ctx) throws ReasonerException {
        super(traversalExecutor, ctx);
        ReasonerQueryFactory queryFactory = context().queryFactory();
        Conjunction<Statement> positiveConj = Graql.and(
                pattern.getPatterns().stream()
                        .filter(p -> !p.isNegation())
                        .flatMap(p -> p.statements().stream())
                        .collect(Collectors.toSet())
        );
        //conjunction of negation patterns
        Set<Conjunction<Pattern>> complementPattern = complementPattern(pattern);
        this.conjunctiveQuery = queryFactory.create(positiveConj);
        this.complementQueries = complementPattern.stream()
                .map(queryFactory::resolvable)
                .collect(Collectors.toSet());

        if (!isNegationSafe()){
            throw ReasonerException.unsafeNegationBlock(this);
        }
    }

    CompositeQuery(ReasonerQueryImpl conj, Set<ResolvableQuery> comp,  TraversalExecutor traversalExecutor, ReasoningContext ctx) {
        super(traversalExecutor, ctx);
        this.conjunctiveQuery = conj;
        this.complementQueries = comp;
    }

    @Override
    public CompositeQuery asComposite() {
        return this;
    }

    @Override
    public DisjunctiveQuery asDisjunctive() {
        throw ReasonerException.illegalQueryConversion(this.getClass(), DisjunctiveQuery.class);
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
                        throw GraqlSemanticException.disjunctiveNegationBlock();
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
     * @param pattern pattern to be validated
     * @return set of error messages applicable
     */
    public static Set<String> validateAsRuleBody(Conjunction<Pattern> pattern, Rule rule, ReasonerQueryFactory reasonerQueryFactory){
        Set<String> errors = new HashSet<>();
        try{
            CompositeQuery body = reasonerQueryFactory.composite(pattern);
            Set<ResolvableQuery> complementQueries = body.getComplementQueries();
            if(complementQueries.size() > 1){
                errors.add(ErrorMessage.VALIDATION_RULE_MULTIPLE_NEGATION_BLOCKS.getMessage(rule.label()));
            }
            if(!body.isPositive() && complementQueries.stream().noneMatch(ReasonerQuery::isPositive)){
                errors.add(ErrorMessage.VALIDATION_RULE_NESTED_NEGATION.getMessage(rule.label()));
            }
        } catch (GraqlSemanticException e) {
            errors.add(ErrorMessage.VALIDATION_RULE_INVALID.getMessage(rule.label(), e.getMessage()));
        }
        return errors;
    }

    @Override
    public Set<String> validateOntologically(Label ruleLabel) {
        Set<String> validation = getConjunctiveQuery().validateOntologically(ruleLabel);
        getComplementQueries().stream().map(q -> q.validateOntologically(ruleLabel)).forEach(validation::addAll);
        return validation;
    }

    @Override
    public CompositeQuery withSubstitution(ConceptMap sub){
        return new CompositeQuery(
                getConjunctiveQuery().withSubstitution(sub),
                getComplementQueries().stream().map(q -> q.withSubstitution(sub)).collect(Collectors.toSet()),
                traversalExecutor,
                context()
        );
    }

    @Override
    public Stream<ConceptMap> traverse(){
        return traversalExecutor.traverse(getPattern()).map(ans -> {
            if (complementQueries.isEmpty()) {
                return new ConceptMap(ans.map(), ans.explanation(), getPattern(ans.map()));
            } else {
                ConceptMap explanationAns = new ConceptMap(ans.map(), new LookupExplanation(), getConjunctiveQuery().getPattern(ans.map()));
                return new ConceptMap(ans.map(), new CompositeExplanation(explanationAns), getPattern(ans.map()));
            }
        });
    }

    @Override
    public CompositeQuery inferTypes() {
        return new CompositeQuery(
                getConjunctiveQuery().inferTypes(),
                getComplementQueries(),
                traversalExecutor,
                context());
    }

    @Override
    public CompositeQuery constantValuePredicateQuery() {
        return new CompositeQuery(
                getConjunctiveQuery().constantValuePredicateQuery(),
                getComplementQueries(),
                traversalExecutor,
                context());
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
                traversalExecutor,
                context()
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
        return getPattern().toString();
    }

    @Override
    public ReasonerQuery conjunction(ReasonerQuery q) {
        HashSet<Pattern> patterns = new HashSet<>(this.getPattern().getPatterns());
        patterns.add(q.getPattern());
        return new CompositeQuery(
                Graql.and(patterns),
                traversalExecutor,
                context()
        );
    }

    @Override
    public void checkValid() {
        getConjunctiveQuery().checkValid();
        getComplementQueries().forEach(ResolvableQuery::checkValid);
    }

    @Override
    public Set<Variable> getVarNames() {
        return getConjunctiveQuery().getVarNames();
    }

    @Override
    public Set<Atomic> getAtoms() {
        Set<Atomic> atoms = new HashSet<>(getConjunctiveQuery().getAtoms());
        getComplementQueries().stream().flatMap(q -> q.getAtoms().stream()).forEach(atoms::add);
        return atoms;
    }

    @Override
    public Conjunction<Pattern> getPattern() {
        if (pattern == null) {
            Set<Pattern> conjunctPatterns = Sets.newLinkedHashSet(getConjunctiveQuery().getPattern().getPatterns());
            getComplementQueries().stream().map(ResolvableQuery::getPattern).forEach(p -> conjunctPatterns.add(Graql.not(p)));
            pattern = Graql.and(conjunctPatterns);
        }
        return pattern;
    }

    @Override
    public Pattern getPattern(Map<Variable, Concept> map){
        HashSet<Pattern> patterns = getIdPredicatePatterns(map);
        patterns.addAll(getPattern().getPatterns());
        return Graql.and(patterns);
    }

    @Override
    public ConceptMap getSubstitution() {
        return getConjunctiveQuery().getSubstitution();
    }

    @Override
    public boolean isRuleResolvable() {
        return getConjunctiveQuery().isRuleResolvable() || getComplementQueries().stream().anyMatch(ResolvableQuery::isRuleResolvable);
    }

    @Override
    public MultiUnifier getMultiUnifier(ReasonerQuery parent) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type getUnambiguousType(Variable var, boolean inferTypes) { throw new UnsupportedOperationException(); }

    @Override
    public ImmutableSetMultimap<Variable, Type> getVarTypeMap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ImmutableSetMultimap<Variable, Type> getVarTypeMap(boolean inferTypes) { return getVarTypeMap(); }

    @Override
    public ImmutableSetMultimap<Variable, Type> getVarTypeMap(ConceptMap sub) { return getVarTypeMap(); }

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
                traversalExecutor,
                context()
        );
    }

    @Override
    public ResolutionState resolutionState(ConceptMap sub, Unifier u, AnswerPropagatorState parent, Set<ReasonerAtomicQuery> subGoals){
        return isPositive()?
                getConjunctiveQuery().resolutionState(sub, u, parent, subGoals) :
                new CompositeState(this, sub, u, parent, subGoals);
    }

    @Override
    public Iterator<ResolutionState> innerStateIterator(AnswerPropagatorState parent, Set<ReasonerAtomicQuery> subGoals) {
        return Iterators.singletonIterator(getConjunctiveQuery().resolutionState(getSubstitution(), parent.getUnifier(), parent, subGoals));
    }

}
