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
import ai.grakn.concept.Type;
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
import ai.grakn.graql.internal.reasoner.atom.NotEquals;
import ai.grakn.graql.internal.reasoner.atom.binary.BinaryBase;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.graql.internal.reasoner.cache.Cache;
import ai.grakn.graql.internal.reasoner.cache.LazyQueryCache;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.iterator.ReasonerQueryIterator;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Comparator;
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

    protected ReasonerQueryImpl(Conjunction<VarPatternAdmin> pattern, GraknGraph graph) {
        this.graph = graph;
        atomSet.addAll(AtomicFactory.createAtomSet(pattern, this));
        inferTypes();
    }

    ReasonerQueryImpl(ReasonerQueryImpl q) {
        this.graph = q.graph;
        q.getAtoms().forEach(at -> addAtomic(AtomicFactory.create(at, this)));
        inferTypes();
    }

    protected ReasonerQueryImpl(Atom atom) {
        if (atom.getParentQuery() == null) {
            throw new IllegalArgumentException(ErrorMessage.PARENT_MISSING.getMessage(atom.toString()));
        }
        this.graph = atom.getParentQuery().graph();
        addAtomic(AtomicFactory.create(atom, this));
        addAtomConstraints(atom.getNonSelectableConstraints());
        inferTypes();
    }

    @Override
    public String toString(){
        return getAtoms().stream().filter(Atomic::isAtom).map(Atomic::toString).collect(Collectors.joining(", "));
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
     *
     * @return
     */
    public int resolutionPriority(){
        if (priority == Integer.MAX_VALUE) {
            Set<Atom> selectableAtoms = selectAtoms();
            int totalPriority = selectableAtoms.stream().mapToInt(Atom::resolutionPriority).sum();
            priority = totalPriority/selectableAtoms.size();
        }
        return priority;
    }

    private void inferTypes() {
        getAtoms().stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .forEach(Atom::inferTypes);
    }

    public GraknGraph graph() {
        return graph;
    }

    public Conjunction<PatternAdmin> getPattern() {
        Set<PatternAdmin> patterns = new HashSet<>();
        atomSet.stream()
                .map(Atomic::getCombinedPattern)
                .flatMap(p -> p.getVars().stream())
                .forEach(patterns::add);
        return Patterns.conjunction(patterns);
    }

    /**
     * @return true if any of the atoms constituting the query can be resolved through a rule
     */
    @Override
    public boolean isRuleResolvable() {
        Iterator<Atom> it = atomSet.stream().filter(Atomic::isAtom).map(at -> (Atom) at).iterator();
        while (it.hasNext()) {
            if (it.next().isRuleResolvable()) {
                return true;
            }
        }
        return false;
    }

    private boolean isTransitive() {
        return getAtoms().stream().filter(this::containsEquivalentAtom).count() == 2;
    }

    boolean isAtomic() {
        return getAtoms().stream().filter(Atomic::isSelectable).count() == 1;
    }

    /**
     * @return atom set constituting this query
     */
    @Override
    public Set<Atomic> getAtoms() { return atomSet;}

    private List<Atom> getPrioritisedAtoms(){
        return selectAtoms().stream()
                .sorted(Comparator.comparing(Atom::resolutionPriority).reversed())
                .collect(Collectors.toList());
    }

    /**
     * @return atom that should be prioritised for resolution
     */
    Atom getTopAtom() {
        return getPrioritisedAtoms().stream().findFirst().orElse(null);
    }

    /**
     * @return resolution plan in a form a atom[priority]->... string
     */
    String getResolutionPlan(){
        return getPrioritisedAtoms().stream().map(at -> at + "[" + at.resolutionPriority()+ "]").collect(Collectors.joining(" -> "));
    }

    /**
     * @return set of id predicates contained in this query
     */
    public Set<IdPredicate> getIdPredicates() {
        return getAtoms().stream()
                .filter(Atomic::isPredicate).map(at -> (Predicate) at)
                .filter(Predicate::isIdPredicate).map(predicate -> (IdPredicate) predicate)
                .collect(Collectors.toSet());
    }

    /**
     * @return set of value predicates contained in this query
     */
    public Set<ValuePredicate> getValuePredicates() {
        return getAtoms().stream()
                .filter(Atomic::isPredicate).map(at -> (Predicate) at)
                .filter(Predicate::isValuePredicate).map(at -> (ValuePredicate) at)
                .collect(Collectors.toSet());
    }

    /**
     * @return set of atoms constituting constraints (by means of types) for this atom
     */
    public Set<TypeAtom> getTypeConstraints() {
        return getAtoms().stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .filter(Atom::isType).map(at -> (TypeAtom) at)
                .collect(Collectors.toSet());
    }

    /**
     * @return set of filter atoms (currently only NotEquals) contained in this query
     */
    public Set<NotEquals> getFilters() {
        return getAtoms().stream()
                .filter(at -> at.getClass() == NotEquals.class)
                .map(at -> (NotEquals) at)
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

    /**
     * @param atom in question
     * @return true if query contains an equivalent atom
     */
    private boolean containsEquivalentAtom(Atomic atom) {
        return !getEquivalentAtoms(atom).isEmpty();
    }

    Set<Atomic> getEquivalentAtoms(Atomic atom) {
        return atomSet.stream().filter(at -> at.isEquivalent(atom)).collect(Collectors.toSet());
    }

    @Override
    public Unifier getUnifier(ReasonerQuery parent) {
        throw new IllegalStateException("Attempted to obtain unifiers on non-atomic queries.");
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
    public Map<Var, Type> getVarTypeMap() {
        Map<Var, Type> typeMap = new HashMap<>();
        getTypeConstraints().stream()
                .filter(at -> Objects.nonNull(at.getType()))
                .forEach(atom -> typeMap.putIfAbsent(atom.getVarName(), atom.getType()));
        return typeMap;
    }

    /**
     * @param var variable name
     * @return id predicate for the specified var name if any
     */
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
     * remove given atom together with its disjoint neighbours (atoms it is connected to)
     * @param atom to be removed
     * @return modified query
     */
    ReasonerQueryImpl removeAtom(Atom atom){
        //selectability may change after removing the top atom so determine first
        Set<TypeAtom> nonSelectableTypes = atom.getTypeConstraints().stream()
                .filter(at -> !at.isSelectable())
                .collect(Collectors.toSet());

        //remove atom of interest
        removeAtomic(atom);

        //remove disjoint type constraints
        nonSelectableTypes.stream()
                .filter(at -> findNextJoinable(at) == null)
                .forEach(this::removeAtomic);

        //remove dangling predicates
        atom.getPredicates().stream()
                .filter(pred -> getVarNames().contains(pred.getVarName()))
                .forEach(this::removeAtomic);
        return this;
    }

    /**
     * adds a set of constraints (types, predicates) to the atom set
     * @param cstrs set of constraints
     */
    public void addAtomConstraints(Set<? extends Atomic> cstrs){
        cstrs.forEach(con -> addAtomic(AtomicFactory.create(con, this)));
    }

    private Atom findFirstJoinable(Set<Atom> atoms){
        for (Atom next : atoms) {
            Atom atom = findNextJoinable(Sets.difference(atoms, Sets.newHashSet(next)), next.getVarNames());
            if (atom != null) return atom;
        }
        return atoms.iterator().next();
    }

    private Atom findNextJoinable(Set<Atom> atoms, Set<Var> vars){
        for (Atom next : atoms) {
            if (!Sets.intersection(vars, next.getVarNames()).isEmpty()) return next;
        }
        return null;
    }

    //TODO move to Atom?
    /**
     * @param atom for which the neighbour is to be found
     * @return neighbour or null if disjoint
     */
    public Atom findNextJoinable(Atom atom){
        Set<Atom> atoms = getAtoms().stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .filter(at -> at != atom)
                .collect(Collectors.toSet());
        return findNextJoinable(atoms, atom.getVarNames());
    }

    /**
     * atom selection function
     * @return selected atoms
     */
    public Set<Atom> selectAtoms() {
        Set<Atom> atoms = getAtoms().stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .collect(Collectors.toSet());
        if (atoms.size() == 1) return atoms;

        //pass relations or rule-resolvable types and resources
        Set<Atom> atomsToSelect = atoms.stream()
                .filter(Atomic::isSelectable)
                .collect(Collectors.toSet());

        Set<Atom> orderedSelection = new LinkedHashSet<>();

        Atom atom = findFirstJoinable(atomsToSelect);
        Set<Var> joinedVars = new HashSet<>();
        while(!atomsToSelect.isEmpty() && atom != null) {
            orderedSelection.add(atom);
            atomsToSelect.remove(atom);
            joinedVars.addAll(atom.getVarNames());
            atom = findNextJoinable(atomsToSelect, joinedVars);
        }
        //if disjoint select at random
        if (!atomsToSelect.isEmpty()) atomsToSelect.forEach(orderedSelection::add);

        if (orderedSelection.isEmpty()) {
            throw new IllegalStateException(ErrorMessage.NO_ATOMS_SELECTED.getMessage(this.toString()));
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
        getTypeConstraints().stream().map(BinaryBase::getValueVariable).forEach(varNames::remove);

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

    private boolean requiresMaterialisation(){
        for(Atom atom : selectAtoms()){
            for (InferenceRule rule : atom.getApplicableRules())
                if (rule.requiresMaterialisation(atom)){
                    return true;
                }
        }
        return false;
    }

    private Stream<Answer> fullJoin(Set<ReasonerAtomicQuery> subGoals,
                                    Cache<ReasonerAtomicQuery, ?> cache,
                                    Cache<ReasonerAtomicQuery, ?> dCache,
                                    boolean materialise,
                                    boolean explanation){
        List<ReasonerAtomicQuery> queries = selectAtoms().stream().map(ReasonerAtomicQuery::new).collect(Collectors.toList());
        Iterator<ReasonerAtomicQuery> qit = queries.iterator();
        ReasonerAtomicQuery childAtomicQuery = qit.next();
        Stream<Answer> join = childAtomicQuery.answerStream(subGoals, cache, dCache, materialise, explanation, false);
        Set<Var> joinedVars = childAtomicQuery.getVarNames();
        while(qit.hasNext()){
            childAtomicQuery = qit.next();
            Set<Var> joinVars = Sets.intersection(joinedVars, childAtomicQuery.getVarNames());
            Stream<Answer> localSubs = childAtomicQuery.answerStream(subGoals, cache, dCache, materialise, explanation, false);
            join = join(join, localSubs, ImmutableSet.copyOf(joinVars), explanation);
            joinedVars.addAll(childAtomicQuery.getVarNames());
        }
        return join;
    }

    private Stream<Answer> differentialJoin(Set<ReasonerAtomicQuery> subGoals,
                                            Cache<ReasonerAtomicQuery, ?> cache,
                                            Cache<ReasonerAtomicQuery, ?> dCache,
                                            boolean materialise,
                                            boolean explanation){
        Stream<Answer> join = Stream.empty();
        List<ReasonerAtomicQuery> queries = selectAtoms().stream().map(ReasonerAtomicQuery::new).collect(Collectors.toList());
        Set<ReasonerAtomicQuery> uniqueQueries = queries.stream().collect(Collectors.toSet());
        //only do one join for transitive queries
        List<ReasonerAtomicQuery> queriesToJoin  = isTransitive()? Lists.newArrayList(uniqueQueries) : queries;

        for(ReasonerAtomicQuery qi : queriesToJoin){
            Stream<Answer> subs = qi.answerStream(subGoals, cache, dCache, materialise, explanation, true);
            Set<Var> joinedVars = qi.getVarNames();
            for(ReasonerAtomicQuery qj : queries){
                if ( qj != qi ){
                    Set<Var> joinVars = Sets.intersection(joinedVars, qj.getVarNames());
                    subs = joinWithInverse(
                            subs,
                            cache.getAnswerStream(qj),
                            cache.getInverseAnswerMap(qj, joinVars),
                            ImmutableSet.copyOf(joinVars),
                            explanation);
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
                               boolean materialise,
                               boolean explanation,
                               boolean differentialJoin) {
        Stream<Answer> join = differentialJoin?
                differentialJoin(subGoals, cache, dCache, materialise, explanation) :
                fullJoin(subGoals, cache, dCache, materialise, explanation);

        Set<NotEquals> filters = getFilters();
        return join
                .filter(a -> nonEqualsFilter(a, filters));
    }

    @Override
    public Stream<Answer> resolve(boolean materialise, boolean explanation) {
        if (materialise || requiresMaterialisation()) {
            return resolve(materialise, explanation, new LazyQueryCache<>(explanation), new LazyQueryCache<>(explanation));
        } else {
            return new QueryAnswerIterator(this).hasStream();
        }
    }

    /**
     * resolves the query
     * @param materialise materialisation flag
     * @return stream of answers
     */
    public Stream<Answer> resolve(boolean materialise, boolean explanation, LazyQueryCache<ReasonerAtomicQuery> cache, LazyQueryCache<ReasonerAtomicQuery> dCache) {

        Iterator<Atom> atIt = this.selectAtoms().iterator();
        ReasonerAtomicQuery atomicQuery = new ReasonerAtomicQuery(atIt.next());
        Stream<Answer> answerStream = atomicQuery.resolve(materialise, explanation, cache, dCache);
        Set<Var> joinedVars = atomicQuery.getVarNames();
        while (atIt.hasNext()) {
            atomicQuery = new ReasonerAtomicQuery(atIt.next());
            Stream<Answer> subAnswerStream = atomicQuery.resolve(materialise, explanation, cache, dCache);
            Set<Var> joinVars = Sets.intersection(joinedVars, atomicQuery.getVarNames());
            answerStream = join(answerStream, subAnswerStream, ImmutableSet.copyOf(joinVars), explanation);
            joinedVars.addAll(atomicQuery.getVarNames());
        }

        Set<NotEquals> filters = this.getFilters();
        Set<Var> vars = this.getVarNames();
        return answerStream
                .filter(a -> nonEqualsFilter(a, filters))
                .map(a -> a.filterVars(vars));
    }

    public ReasonerQueryIterator iterator(Answer sub, Set<ReasonerAtomicQuery> subGoals, QueryCache<ReasonerAtomicQuery> cache){
        return new ReasonerQueryImplIterator(this, sub, subGoals, cache);
    }
}
