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

import com.google.common.collect.SetMultimap;
import grakn.core.common.util.LazyMergingStream;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.ResolutionIterator;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.explanation.DisjunctiveExplanation;
import grakn.core.graql.reasoner.explanation.LookupExplanation;
import grakn.core.graql.reasoner.state.AnswerPropagatorState;
import grakn.core.graql.reasoner.state.DisjunctiveState;
import grakn.core.graql.reasoner.state.ResolutionState;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.graql.executor.TraversalExecutor;
import grakn.core.kb.graql.reasoner.ReasonerException;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Disjunction;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Variable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DisjunctiveQuery extends ResolvableQuery {
    private final Set<ResolvableQuery> clauses;
    private final Set<Variable> bindingVars;

    public DisjunctiveQuery(Disjunction<Conjunction<Pattern>> pattern, Set<Variable> bindingVars, TraversalExecutor traversalExecutor, ReasoningContext ctx) {
        super(traversalExecutor, ctx);
        ReasonerQueryFactory queryFactory = context().queryFactory();
        clauses = pattern.getPatterns().stream().map(queryFactory::composite).collect(Collectors.toSet());
        this.bindingVars = bindingVars;
    }

    public DisjunctiveQuery(Set<ResolvableQuery> clauses, Set<Variable> bindingVars, TraversalExecutor traversalExecutor, ReasoningContext ctx) {
        super(traversalExecutor, ctx);
        this.clauses = clauses;
        this.bindingVars = bindingVars;
    }

    @Override
    public CompositeQuery asComposite() {
        throw ReasonerException.illegalQueryConversion(this.getClass(), CompositeQuery.class);
    }

    @Override
    public DisjunctiveQuery asDisjunctive() {
        return this;
    }

    public Set<ResolvableQuery> getClauses() {
        return new HashSet<>(clauses);
    }

    public Set<Variable> getBindingVars() {
        return new HashSet<>(bindingVars);
    }

    // Only needed for a disjunction to be the body of a rule
    @Override
    public Set<String> validateOntologically(Label ruleLabel) {
        throw new UnsupportedOperationException("A disjunctions in rule body is not yet supported");
    }

    public HashMap<Variable, Concept> filterBindingVars(Map<Variable, Concept> map) {

        HashMap<Variable, Concept> bindingVarsMap = new HashMap<>();

        bindingVars.forEach(b -> {
            if (map.get(b) != null) {
                bindingVarsMap.put(b, map.get(b));
            }
        });

        return bindingVarsMap;
    }

    @Override
    public ResolvableQuery withSubstitution(ConceptMap sub) {
        return new DisjunctiveQuery(
                getClauses().stream().map(q -> q.withSubstitution(sub)).collect(Collectors.toSet()),
                getBindingVars(),
                traversalExecutor,
                context()
        );
    }

    @Override
    public Stream<ConceptMap> traverse(){
        Stream<Stream<ConceptMap>> answerStreams = clauses.stream().map(clause ->
                clause.traverse().map(ans -> {
                    ConceptMap clauseAns = new ConceptMap(ans.map(), new LookupExplanation(), clause.getPattern(ans.map()));
                    HashMap<Variable, Concept> bindingVarsSub = filterBindingVars(ans.map());
                    return new ConceptMap(bindingVarsSub, new DisjunctiveExplanation(clauseAns), getPattern(bindingVarsSub));
                }));
        LazyMergingStream<ConceptMap> mergedStreams = new LazyMergingStream<>(answerStreams);
        return mergedStreams.flatStream();
    }

    @Override
    ResolvableQuery inferTypes() {
        return new DisjunctiveQuery(
                getClauses().stream().map(ResolvableQuery::inferTypes).collect(Collectors.toSet()),
                getBindingVars(),
                traversalExecutor,
                context()
        );
    }

    @Override
    ResolvableQuery constantValuePredicateQuery() {
        return new DisjunctiveQuery(
                getClauses().stream().map(ResolvableQuery::constantValuePredicateQuery).collect(Collectors.toSet()),
                getBindingVars(),
                traversalExecutor,
                context()
        );
    }

    @Override
    ResolvableQuery copy() {
        return new DisjunctiveQuery(
                getClauses(),
                getBindingVars(),
                traversalExecutor,
                context());
    }

    @Override
    public boolean isAtomic() {
        // TODO unclear of the meaning of atomicity in this case
        return getClauses().stream().allMatch(ResolvableQuery::isAtomic);
    }

    @Override
    boolean isEquivalent(ResolvableQuery q) {
        DisjunctiveQuery that = q.asDisjunctive();
        return that.getClauses().size() == getClauses().size()
                && getClauses().stream().allMatch(clause -> that.getClauses().stream().anyMatch(clause::isEquivalent));
    }

    @Override
    public String toString(){
        return getPattern().toString();
    }

    @Override
    public ReasonerQuery conjunction(ReasonerQuery q) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkValid() {
        clauses.forEach(ResolvableQuery::checkValid);
    }

    @Override
    public Set<Variable> getVarNames() {
        return bindingVars;
    }

    @Override
    public Set<Atomic> getAtoms() {
        return getClauses().stream().flatMap(c -> c.getAtoms().stream()).collect(Collectors.toSet());
    }

    @Override
    public Disjunction<Pattern> getPattern() {
        return Graql.or(clauses.stream().map(ResolvableQuery::getPattern).collect(Collectors.toSet()));
    }

    @Override
    public Conjunction<Pattern> getPattern(Map<Variable, Concept> map) {
        HashSet<Pattern> patterns = getIdPredicatePatterns(map);
        patterns.add(getPattern());
        return Graql.and(patterns);
    }

    @Override
    public ConceptMap getSubstitution() {
        // When this method is needed it should be a substitution over the binding variables
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRuleResolvable() {
        return clauses.stream().anyMatch(ResolvableQuery::isRuleResolvable);
    }

    @Override
    public MultiUnifier getMultiUnifier(ReasonerQuery parent) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type getUnambiguousType(Variable var, boolean inferTypes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SetMultimap<Variable, Type> getVarTypeMap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SetMultimap<Variable, Type> getVarTypeMap(boolean inferTypes) {
        return getVarTypeMap();
    }

    @Override
    public SetMultimap<Variable, Type> getVarTypeMap(ConceptMap sub) {
        return getVarTypeMap();
    }

    @Override
    public boolean requiresReiteration() {
        return clauses.stream().anyMatch(ResolvableQuery::requiresReiteration);
    }

    @Override
    public Stream<Atom> selectAtoms() {
        return getAtoms(Atom.class).filter(Atomic::isSelectable);
    }

    @Override
    public boolean requiresDecomposition() {
        return clauses.stream().anyMatch(ResolvableQuery::requiresDecomposition);
    }

    @Override
    public DisjunctiveQuery rewrite() {
        return new DisjunctiveQuery(
                clauses.stream().map(ResolvableQuery::rewrite).collect(Collectors.toSet()),
                getBindingVars(),
                traversalExecutor,
                context());
    }

    @Override
    public ResolutionState resolutionState(ConceptMap sub, Unifier u, AnswerPropagatorState parent, Set<ReasonerAtomicQuery> subGoals) {
        return new DisjunctiveState(this, sub, u, parent, subGoals);
    }

    @Override
    public Iterator<ResolutionState> innerStateIterator(AnswerPropagatorState parent, Set<ReasonerAtomicQuery> subGoals) {
        return clauses.stream().map(c -> c.resolutionState(c.getSubstitution(), parent.getUnifier(), parent, subGoals)).iterator();
    }

}
