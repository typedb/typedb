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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.MatchQuery;
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
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.binary.Binary;
import ai.grakn.graql.internal.reasoner.atom.binary.RelationAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.NeqPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.graql.internal.reasoner.cache.Cache;
import ai.grakn.graql.internal.reasoner.cache.LazyQueryCache;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
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

    private final GraknGraph graph;
    private final Set<Atomic> atomSet = new HashSet<>();
    private int priority = Integer.MAX_VALUE;

    ReasonerQueryImpl(Conjunction<VarPatternAdmin> pattern, GraknGraph graph) {
        this.graph = graph;
        atomSet.addAll(AtomicFactory.createAtomSet(pattern, this));
        inferTypes();
    }

    ReasonerQueryImpl(ReasonerQueryImpl q) {
        this.graph = q.graph;
        q.getAtoms().forEach(at -> addAtomic(AtomicFactory.create(at, this)));
    }

    ReasonerQueryImpl(Set<Atom> atoms, GraknGraph graph){
        this.graph = graph;

        atoms.stream()
                .map(at -> AtomicFactory.create(at, this))
                .forEach(this::addAtomic);
        atoms.stream()
                .map(Atom::getNonSelectableConstraints)
                .forEach(this::addAtomConstraints);
        inferTypes();
    }

    ReasonerQueryImpl(Atom atom) {
        this(Collections.singleton(atom), atom.getParentQuery().graph());
    }

    @Override
    public String toString(){
        return "{\n" +
                atomSet.stream()
                        .filter(Atomic::isAtom)
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
        atomSet.forEach(atom -> hashes.add(atom.equivalenceHashCode()));
        for (Integer hash : hashes) hashCode = hashCode * 37 + hash;
        return hashCode;
    }

    /**
     * @return the normalised priority of this query based on its atom content
     */
    public int resolutionPriority(){
        if (priority == Integer.MAX_VALUE) {
            Set<Atom> selectableAtoms = selectAtoms();
            int totalPriority = selectableAtoms.stream().mapToInt(Atom::baseResolutionPriority).sum();
            priority = totalPriority/selectableAtoms.size();
        }
        return priority;
    }

    /**
     * replace all atoms with inferrable types with their new instances with added types
     */
    private void inferTypes() {
        Set<Atom> inferrableAtoms = atomSet.stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .collect(Collectors.toSet());
        Set<Atom> inferredAtoms = inferrableAtoms.stream().map(Atom::inferTypes).collect(Collectors.toSet());
        inferrableAtoms.forEach(this::removeAtomic);
        inferredAtoms.forEach(this::addAtomic);
    }

    @Override
    public GraknGraph graph() {
        return graph;
    }

    @Override
    public Conjunction<PatternAdmin> getPattern() {
        Set<PatternAdmin> patterns = new HashSet<>();
        atomSet.stream()
                .map(Atomic::getCombinedPattern)
                .flatMap(p -> p.getVars().stream())
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
        return atomSet.stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .filter(this::containsEquivalentAtom).count() == 2;
    }

    boolean isAtomic() {
        return atomSet.stream().filter(Atomic::isSelectable).count() == 1;
    }

    /**
     * @return atom set constituting this query
     */
    @Override
    public Set<Atomic> getAtoms() { return atomSet;}

    /**
     * @return set of predicates contained in this query
     */
    public Set<Predicate> getPredicates() {
        return getAtoms().stream()
                .filter(Atomic::isPredicate).map(at -> (Predicate) at)
                .collect(Collectors.toSet());
    }

    /**
     * @return set of id predicates contained in this query
     */
    public Set<IdPredicate> getIdPredicates() {
        return getPredicates().stream()
                .filter(Predicate::isIdPredicate).map(predicate -> (IdPredicate) predicate)
                .collect(Collectors.toSet());
    }

    /**
     * @return set of value predicates contained in this query
     */
    public Set<ValuePredicate> getValuePredicates() {
        return getPredicates().stream()
                .filter(Predicate::isValuePredicate).map(at -> (ValuePredicate) at)
                .collect(Collectors.toSet());
    }

    /**
     * @return set of neq predicates contained in this query
     */
    private Set<NeqPredicate> getNeqPredicates() {
        return getPredicates().stream()
                .filter(Predicate::isNeqPredicate).map(at -> (NeqPredicate) at)
                .collect(Collectors.toSet());
    }

    /**
     * @return set of atoms constituting constraints (by means of types) for this query
     */
    public Set<TypeAtom> getTypeConstraints() {
        return getAtoms().stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .filter(Atom::isType).map(at -> (TypeAtom) at)
                .collect(Collectors.toSet());
    }

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
     * @return corresponding MatchQuery
     */
    @Override
    public MatchQuery getMatchQuery() {
        return graph.graql().infer(false).match(getPattern());
    }

    /**
     * @return map of variable name - type pairs
     */
    @Override
    public Map<Var, OntologyConcept> getVarOntologyConceptMap() {
        Map<Var, OntologyConcept> typeMap = new HashMap<>();
        getTypeConstraints().stream()
                .filter(at -> Objects.nonNull(at.getOntologyConcept()))
                .forEach(atom -> typeMap.putIfAbsent(atom.getVarName(), atom.getOntologyConcept()));
        return typeMap;
    }

    /**
     * @param var variable name
     * @return id predicate for the specified var name if any
     */
    @Nullable
    public IdPredicate getIdPredicate(Var var) {
        Set<IdPredicate> relevantSubs = getIdPredicates().stream()
                .filter(sub -> sub.getVarName().equals(var))
                .collect(Collectors.toSet());
        return relevantSubs.isEmpty() ? null : relevantSubs.iterator().next();
    }

    /**
     * @param atom to be added
     * @return true if the atom set did not already contain the specified atom
     */
    public boolean addAtomic(Atomic atom) {
        if (atomSet.add(atom)) {
            atom.setParentQuery(this);
            return true;
        } else return false;
    }

    /**
     * @param atom to be removed
     * @return true if the atom set contained the specified atom
     */
    public boolean removeAtomic(Atomic atom) {
        return atomSet.remove(atom);
    }

    /**
     * adds a set of constraints (types, predicates) to the atom set
     * @param cstrs set of constraints
     */
    public void addAtomConstraints(Set<? extends Atomic> cstrs){
        cstrs.forEach(con -> addAtomic(AtomicFactory.create(con, this)));
    }

    /**
     * atom selection function
     * @return selected atoms
     */
    public Set<Atom> selectAtoms() {
        Set<Atom> atomsToSelect = atomSet.stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .filter(Atomic::isSelectable)
                .collect(Collectors.toSet());
        if (atomsToSelect.size() <= 2) return atomsToSelect;

        Set<Atom> orderedSelection = new LinkedHashSet<>();

        Atom atom = atomsToSelect.stream()
                .filter(at -> at.getNeighbours().findFirst().isPresent())
                .findFirst().orElse(null);
        while(!atomsToSelect.isEmpty() && atom != null) {
            orderedSelection.add(atom);
            atomsToSelect.remove(atom);
            atom = atom.getNeighbours()
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
     * @param q query to be compared with
     * @return true if two queries are alpha-equivalent
     */
    public boolean isEquivalent(ReasonerQueryImpl q) {
        Set<Atom> atoms = atomSet.stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .collect(Collectors.toSet());
        if(atoms.size() != q.getAtoms().stream().filter(Atomic::isAtom).count()) return false;
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
        return atomSet.stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .filter(at -> at.isEquivalent(atom))
                .collect(Collectors.toSet());
    }

    /**
     * @return substitution obtained from all id predicates (including internal) in the query
     */
    Answer getSubstitution(){
        Set<IdPredicate> predicates = this.getTypeConstraints().stream()
                .map(TypeAtom::getPredicate)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        predicates.addAll(getIdPredicates());

        // the mapping function is declared separately to please the Eclipse compiler
        Function<IdPredicate, Concept> f = p -> graph().getConcept(p.getPredicate());

        return new QueryAnswer(predicates.stream()
                .collect(Collectors.toMap(IdPredicate::getVarName, f))
        );
    }

    ReasonerQueryImpl addSubstitution(Answer sub){
        Set<Var> varNames = getVarNames();

        //skip predicates from types
        getTypeConstraints().stream().map(Binary::getPredicateVariable).forEach(varNames::remove);

        Set<IdPredicate> predicates = sub.entrySet().stream()
                .filter(e -> varNames.contains(e.getKey()))
                .map(e -> new IdPredicate(e.getKey(), e.getValue(), this))
                .collect(Collectors.toSet());
        atomSet.addAll(predicates);

        return this;
    }

    boolean hasFullSubstitution(){
        return getSubstitution().keySet().containsAll(getVarNames());
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

        Set<NeqPredicate> neqPredicates = getNeqPredicates();
        neqPredicates.forEach(this::removeAtomic);

        Stream<Answer> join = differentialJoin?
                differentialJoin(subGoals, cache, dCache) :
                fullJoin(subGoals, cache, dCache);

        return join.filter(a -> nonEqualsFilter(a, neqPredicates));
    }

    @Override
    public Stream<Answer> resolve(boolean materialise) {
        if (materialise) {
            return resolveAndMaterialise(new LazyQueryCache<>(), new LazyQueryCache<>());
        } else {
            return new QueryAnswerIterator(this).hasStream();
        }
    }

    /**
     * resolves the query and materialises answers, explanations are not provided
     * @return stream of answers
     */
    Stream<Answer> resolveAndMaterialise(LazyQueryCache<ReasonerAtomicQuery> cache, LazyQueryCache<ReasonerAtomicQuery> dCache) {

        Set<NeqPredicate> neqPredicates = getNeqPredicates();
        neqPredicates.forEach(this::removeAtomic);

        Iterator<Atom> atIt = this.selectAtoms().iterator();
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
                .map(a -> a.filterVars(vars));
    }

    /**
     * @param sub partial substitution if any
     * @param subGoals visited subGoals
     * @param cache query cache
     * @return answer iterator from this query
     */
    public Iterator<Answer> iterator(Answer sub, Set<ReasonerAtomicQuery> subGoals, QueryCache<ReasonerAtomicQuery> cache) {
        return new ReasonerQueryImplIterator(this, sub, subGoals, cache);
    }

    /**
     * @param sub partial substitution if any
     * @param subGoals visited subGoals
     * @param cache query cache
     * @return answer iterator from this query obtained by expanding the query by inferred types
     */
    public Iterator<Answer> extendedIterator(Answer sub, Set<ReasonerAtomicQuery> subGoals, QueryCache<ReasonerAtomicQuery> cache){
        Iterator<ReasonerQueryImplIterator> qIterator = getQueryStream(sub)
                .map(q -> new ReasonerQueryImplIterator(q, sub, subGoals, cache))
                .iterator();
        return Iterators.concat(qIterator);
    }

    /**
     * @return stream of queries obtained by inserting all inferred possible types (if ambiguous)
     */
    private Stream<ReasonerQueryImpl> getQueryStream(Answer sub){
        List<Set<Atom>> atomOptions = getAtoms().stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .map(at -> {
                    if (at.isRelation() && at.getOntologyConcept() == null) {
                        RelationAtom rel = (RelationAtom) at;
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
                .map(atomList -> ReasonerQueries.create(new HashSet<>(atomList), graph()));
    }
}
