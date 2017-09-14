/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

package ai.grakn.graql.internal.reasoner.query;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.ResolutionIterator;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.binary.RelationshipAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.NeqPredicate;
import ai.grakn.graql.internal.reasoner.cache.Cache;
import ai.grakn.graql.internal.reasoner.cache.LazyQueryCache;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.reasoner.rule.RuleUtil;
import ai.grakn.graql.internal.reasoner.state.ConjunctiveState;
import ai.grakn.graql.internal.reasoner.state.QueryState;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.join;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.joinWithInverse;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.nonEqualsFilter;

/**
 *
 * <p>
 * Base reasoner query providing resolution and atom handling facilities for conjunctive graql queries.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class ReasonerQueryImpl implements ReasonerQuery {

    private final GraknTx tx;
    private final ImmutableSet<Atomic> atomSet;

    ReasonerQueryImpl(Conjunction<VarPatternAdmin> pattern, GraknTx tx) {
        this.tx = tx;
        this.atomSet = ImmutableSet.<Atomic>builder()
                .addAll(AtomicFactory.createAtoms(pattern, this).iterator())
                .build();
    }

    ReasonerQueryImpl(List<Atom> atoms, GraknTx tx){
        this.tx = tx;
        this.atomSet =  ImmutableSet.<Atomic>builder()
                .addAll(atoms.stream()
                        .flatMap(at -> Stream.concat(Stream.of(at), at.getNonSelectableConstraints()))
                        .map(at -> AtomicFactory.create(at, this)).iterator())
                .build();
    }

    ReasonerQueryImpl(Set<Atomic> atoms, GraknTx tx){
        this.tx = tx;
        this.atomSet = ImmutableSet.<Atomic>builder()
                .addAll(atoms.stream().map(at -> AtomicFactory.create(at, this)).iterator())
                .build();
    }

    ReasonerQueryImpl(Atom atom) {
        this(Collections.singletonList(atom), atom.getParentQuery().tx());
    }

    ReasonerQueryImpl(ReasonerQueryImpl q) {
        this.tx = q.tx;
        this.atomSet =  ImmutableSet.<Atomic>builder()
                .addAll(q.getAtoms().stream().map(at -> AtomicFactory.create(at, this)).iterator())
                .build();
    }

    /**
     * @return corresponding reasoner query with inferred types
     */
    public ReasonerQueryImpl inferTypes() {
        return new ReasonerQueryImpl(getAtoms().stream().map(Atomic::inferTypes).collect(Collectors.toSet()), tx());
    }

    /**
     * @return corresponding positive query (with neq predicates removed)
     */
    public ReasonerQueryImpl positive(){
        return new ReasonerQueryImpl(getAtoms().stream().filter(at -> !(at instanceof NeqPredicate)).collect(Collectors.toSet()), tx());
    }

    @Override
    public String toString(){
        return "{\n" +
                getAtoms(Atom.class)
                        .filter(Atomic::isSelectable)
                        .map(Atomic::toString)
                        .collect(Collectors.joining(";\n")) +
                "\n}\n";
    }

    @Override
    public ReasonerQuery copy() {
        return new ReasonerQueryImpl(this);
    }

    //alpha-equivalence equality
    @Override
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        ReasonerQueryImpl a2 = (ReasonerQueryImpl) obj;
        return this.isEquivalent(a2);
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        SortedSet<Integer> hashes = new TreeSet<>();
        getAtoms().forEach(atom -> hashes.add(atom.equivalenceHashCode()));
        for (Integer hash : hashes) hashCode = hashCode * 37 + hash;
        return hashCode;
    }

    /**
     * @param q query to be compared with
     * @return true if two queries are alpha-equivalent
     */
    public boolean isEquivalent(ReasonerQueryImpl q) {
        if(getAtoms().size() != q.getAtoms().size()) return false;
        Set<Atom> atoms = getAtoms(Atom.class).collect(Collectors.toSet());
        for (Atom atom : atoms){
            if(!q.containsEquivalentAtom(atom)){
                return false;
            }
        }
        return true;
    }

    /**
     * @param atom in question
     * @return true if query contains an equivalent atom
     */
    private boolean containsEquivalentAtom(Atom atom) {
        return !getEquivalentAtoms(atom).isEmpty();
    }

    Set<Atom> getEquivalentAtoms(Atom atom) {
        return getAtoms(Atom.class)
                .filter(at -> at.isEquivalent(atom))
                .collect(Collectors.toSet());
    }

    @Override
    public GraknTx tx() {
        return tx;
    }

    @Override
    public Conjunction<PatternAdmin> getPattern() {
        Set<PatternAdmin> patterns = new HashSet<>();
        atomSet.stream()
                .map(Atomic::getCombinedPattern)
                .flatMap(p -> p.varPatterns().stream())
                .forEach(patterns::add);
        return Patterns.conjunction(patterns);
    }

    @Override
    public Set<String> validateOntologically() {
        return getAtoms().stream()
                .flatMap(at -> at.validateOntologically().stream())
                .collect(Collectors.toSet());
    }

    /**
     * @return true if any of the atoms constituting the query can be resolved through a rule
     */
    @Override
    public boolean isRuleResolvable() {
        for (Atom atom : selectAtoms()) {
            if (atom.isRuleResolvable()) {
                return true;
            }
        }
        return false;
    }

    private boolean isTransitive() {
        return getAtoms(Atom.class).filter(this::containsEquivalentAtom).count() == 2;
    }

    /**
     * @return true if this query is atomic
     */
    boolean isAtomic() {
        return atomSet.stream().filter(Atomic::isSelectable).count() == 1;
    }

    /**
     * @return atom set defining this reasoner query
     */
    @Override
    public Set<Atomic> getAtoms() { return atomSet;}

    /**
     * @param type the class of {@link Atomic} to return
     * @param <T> the type of {@link Atomic} to return
     * @return stream of atoms of specified type defined in this query
     */
    @Override
    public <T extends Atomic> Stream<T> getAtoms(Class<T> type) {
        return atomSet.stream().filter(type::isInstance).map(type::cast);}

    /**
     * @return set of variables appearing in this query
     */
    @Override
    public Set<Var> getVarNames() {
        Set<Var> vars = new HashSet<>();
        atomSet.forEach(atom -> vars.addAll(atom.getVarNames()));
        return vars;
    }

    @Override
    public Unifier getUnifier(ReasonerQuery parent) {
        throw GraqlQueryException.getUnifierOfNonAtomicQuery();
    }

    /**
     * @return corresponding {@link GetQuery}
     */
    @Override
    public GetQuery getQuery() {
        return tx.graql().infer(false).match(getPattern()).get();
    }

    /**
     * @return map of variable name - type pairs
     */
    @Override
    public Map<Var, SchemaConcept> getVarSchemaConceptMap() {
        Map<Var, SchemaConcept> typeMap = new HashMap<>();
        getAtoms(TypeAtom.class)
                .filter(at -> Objects.nonNull(at.getSchemaConcept()))
                .forEach(atom -> typeMap.putIfAbsent(atom.getVarName(), atom.getSchemaConcept()));
        return typeMap;
    }

    /**
     * @param var variable name
     * @return id predicate for the specified var name if any
     */
    @Nullable
    public IdPredicate getIdPredicate(Var var) {
        return getAtoms(IdPredicate.class)
                .filter(sub -> sub.getVarName().equals(var))
                .findFirst().orElse(null);
    }

    /**
     * atom selection function
     * @return selected atoms
     */
    public Set<Atom> selectAtoms() {
        Set<Atom> atomsToSelect = getAtoms(Atom.class)
                .filter(Atomic::isSelectable)
                .collect(Collectors.toSet());
        if (atomsToSelect.size() <= 2) return atomsToSelect;

        Set<Atom> orderedSelection = new LinkedHashSet<>();

        Atom atom = atomsToSelect.stream()
                .filter(at -> at.getNeighbours(Atom.class).findFirst().isPresent())
                .findFirst().orElse(null);
        while(!atomsToSelect.isEmpty() && atom != null) {
            orderedSelection.add(atom);
            atomsToSelect.remove(atom);
            atom = atom.getNeighbours(Atom.class)
                    .filter(atomsToSelect::contains)
                    .findFirst().orElse(null);
        }
        //if disjoint select at random
        if (!atomsToSelect.isEmpty()) atomsToSelect.forEach(orderedSelection::add);

        if (orderedSelection.isEmpty()) {
            throw GraqlQueryException.noAtomsSelected(this);
        }
        return orderedSelection;
    }

    /**
     * @return substitution obtained from all id predicates (including internal) in the query
     */
    public Answer getSubstitution(){
        Set<IdPredicate> predicates = getAtoms(TypeAtom.class)
                .map(TypeAtom::getTypePredicate)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        getAtoms(IdPredicate.class).forEach(predicates::add);

        // the mapping function is declared separately to please the Eclipse compiler
        Function<IdPredicate, Concept> f = p -> tx().getConcept(p.getPredicate());

        return new QueryAnswer(predicates.stream().collect(Collectors.toMap(IdPredicate::getVarName, f)));
    }

    public Answer getRoleSubstitution(){
        Answer answer = new QueryAnswer();
        getAtoms(RelationshipAtom.class)
                .flatMap(RelationshipAtom::getRolePredicates)
                .forEach(p -> answer.put(p.getVarName(), tx().getConcept(p.getPredicate())));
        return answer;
    }

    /**
     * @return true if this query is a ground query
     */
    public boolean isGround(){
        return getSubstitution().vars().containsAll(getVarNames());
    }

    private Stream<Answer> fullJoin(Set<ReasonerAtomicQuery> subGoals,
                                    Cache<ReasonerAtomicQuery, ?> cache,
                                    Cache<ReasonerAtomicQuery, ?> dCache){
        List<ReasonerAtomicQuery> queries = selectAtoms().stream().map(ReasonerAtomicQuery::new).collect(Collectors.toList());
        Iterator<ReasonerAtomicQuery> qit = queries.iterator();
        ReasonerAtomicQuery childAtomicQuery = qit.next();
        Stream<Answer> join = childAtomicQuery.answerStream(subGoals, cache, dCache, false);
        Set<Var> joinedVars = childAtomicQuery.getVarNames();
        while(qit.hasNext()){
            childAtomicQuery = qit.next();
            Set<Var> joinVars = Sets.intersection(joinedVars, childAtomicQuery.getVarNames());
            Stream<Answer> localSubs = childAtomicQuery.answerStream(subGoals, cache, dCache, false);
            join = join(join, localSubs, ImmutableSet.copyOf(joinVars));
            joinedVars.addAll(childAtomicQuery.getVarNames());
        }
        return join;
    }

    private Stream<Answer> differentialJoin(Set<ReasonerAtomicQuery> subGoals,
                                            Cache<ReasonerAtomicQuery, ?> cache,
                                            Cache<ReasonerAtomicQuery, ?> dCache
                                            ){
        Stream<Answer> join = Stream.empty();
        List<ReasonerAtomicQuery> queries = selectAtoms().stream().map(ReasonerAtomicQuery::new).collect(Collectors.toList());
        Set<ReasonerAtomicQuery> uniqueQueries = queries.stream().collect(Collectors.toSet());
        //only do one join for transitive queries
        List<ReasonerAtomicQuery> queriesToJoin  = isTransitive()? Lists.newArrayList(uniqueQueries) : queries;

        for(ReasonerAtomicQuery qi : queriesToJoin){
            Stream<Answer> subs = qi.answerStream(subGoals, cache, dCache, true);
            Set<Var> joinedVars = qi.getVarNames();
            for(ReasonerAtomicQuery qj : queries){
                if ( qj != qi ){
                    Set<Var> joinVars = Sets.intersection(joinedVars, qj.getVarNames());
                    subs = joinWithInverse(
                            subs,
                            cache.getAnswerStream(qj),
                            cache.getInverseAnswerMap(qj, joinVars),
                            ImmutableSet.copyOf(joinVars));
                    joinedVars.addAll(qj.getVarNames());
                }
            }
            join = Stream.concat(join, subs);
        }
        return join;
    }

    Stream<Answer> computeJoin(Set<ReasonerAtomicQuery> subGoals,
                               Cache<ReasonerAtomicQuery, ?> cache,
                               Cache<ReasonerAtomicQuery, ?> dCache,
                               boolean differentialJoin) {

        //handling neq predicates by using complement
        Set<NeqPredicate> neqPredicates = getAtoms(NeqPredicate.class).collect(Collectors.toSet());
        ReasonerQueryImpl positive = neqPredicates.isEmpty()? this : this.positive();

        Stream<Answer> join = differentialJoin?
                positive.differentialJoin(subGoals, cache, dCache) :
                positive.fullJoin(subGoals, cache, dCache);

        return join.filter(a -> nonEqualsFilter(a, neqPredicates));
    }

    @Override
    public Stream<Answer> resolve(boolean materialise) {
        if (materialise) {
            return resolveAndMaterialise(new LazyQueryCache<>(), new LazyQueryCache<>());
        } else {
            return new ResolutionIterator(this).hasStream();
        }
    }

    /**
     * resolves the query and materialises answers, explanations are not provided
     * @return stream of answers
     */
    Stream<Answer> resolveAndMaterialise(LazyQueryCache<ReasonerAtomicQuery> cache, LazyQueryCache<ReasonerAtomicQuery> dCache) {

        //handling neq predicates by using complement
        Set<NeqPredicate> neqPredicates = getAtoms(NeqPredicate.class).collect(Collectors.toSet());
        ReasonerQueryImpl positive = neqPredicates.isEmpty()? this : this.positive();

        Iterator<Atom> atIt = positive.selectAtoms().iterator();
        ReasonerAtomicQuery atomicQuery = new ReasonerAtomicQuery(atIt.next());
        Stream<Answer> answerStream = atomicQuery.resolveAndMaterialise(cache, dCache);
        Set<Var> joinedVars = atomicQuery.getVarNames();

        while (atIt.hasNext()) {
            atomicQuery = new ReasonerAtomicQuery(atIt.next());
            Stream<Answer> subAnswerStream = atomicQuery.resolveAndMaterialise(cache, dCache);
            Set<Var> joinVars = Sets.intersection(joinedVars, atomicQuery.getVarNames());
            answerStream = join(answerStream, subAnswerStream, ImmutableSet.copyOf(joinVars));
            joinedVars.addAll(atomicQuery.getVarNames());
        }

        Set<Var> vars = this.getVarNames();
        return answerStream
                .filter(a -> nonEqualsFilter(a, neqPredicates))
                .map(a -> a.project(vars));
    }

    /**
     * @param sub partial substitution
     * @param u unifier with parent state
     * @param parent parent state
     * @param subGoals set of visited sub goals
     * @param cache query cache
     * @return resolution subGoal formed from this query
     */
    public QueryState subGoal(Answer sub, Unifier u, QueryState parent, Set<ReasonerAtomicQuery> subGoals, QueryCache<ReasonerAtomicQuery> cache){
        return new ConjunctiveState(this, sub, u, parent, subGoals, cache);
    }

    /**
     * @param sub partial substitution
     * @param u unifier with parent state
     * @param parent parent state
     * @param subGoals set of visited sub goals
     * @param cache query cache
     * @return resolution subGoals formed from this query obtained by expanding the inferred types contained in the query
     */
    public LinkedList<QueryState> subGoals(Answer sub, Unifier u, QueryState parent, Set<ReasonerAtomicQuery> subGoals, QueryCache<ReasonerAtomicQuery> cache){
        return getQueryStream(sub)
                .map(q -> q.subGoal(sub, u, parent, subGoals, cache))
                .collect(Collectors.toCollection(LinkedList::new));
    }

    /**
     * @return stream of queries obtained by inserting all inferred possible types (if ambiguous)
     */
    Stream<ReasonerQueryImpl> getQueryStream(Answer sub){
        List<Set<Atom>> atomOptions = getAtoms(Atom.class)
                .map(at -> {
                    if (at.isRelation() && at.getSchemaConcept() == null) {
                        RelationshipAtom rel = (RelationshipAtom) at;
                        Set<Atom> possibleRels = new HashSet<>();
                        rel.inferPossibleRelationTypes(sub).stream()
                                .map(rel::addType)
                                .forEach(possibleRels::add);
                        return possibleRels;
                    } else {
                        return Collections.singleton(at);
                    }
                })
                .collect(Collectors.toList());

        if (atomOptions.stream().mapToInt(Set::size).sum() == atomOptions.size()) {
            return Stream.of(this);
        }

        return Sets.cartesianProduct(atomOptions).stream()
                .map(atomList -> ReasonerQueries.create(atomList, tx()));
    }

    /**
     * reiteration might be required if rule graph contains loops with negative flux
     * or there exists a rule which head satisfies body
     * @return true if because of the rule graph form, the resolution of this query may require reiteration
     */
    public boolean requiresReiteration() {
        Set<InferenceRule> dependentRules = RuleUtil.getDependentRules(this);
        return RuleUtil.subGraphHasLoops(dependentRules, tx())
               || RuleUtil.subGraphHasRulesWithHeadSatisfyingBody(dependentRules);
    }
}
