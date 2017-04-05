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
import ai.grakn.concept.Type;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.Utility;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.NotEquals;
import ai.grakn.graql.internal.reasoner.atom.binary.BinaryBase;
import ai.grakn.graql.internal.reasoner.atom.binary.Resource;
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
import com.google.common.collect.ImmutableMap;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ai.grakn.graql.internal.reasoner.Utility.uncapture;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.join;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.joinWithInverse;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.nonEqualsFilter;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.varFilterFunction;

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
    private static final Logger LOG = LoggerFactory.getLogger(ReasonerQueryImpl.class);

    public ReasonerQueryImpl(Conjunction<VarAdmin> pattern, GraknGraph graph) {
        this.graph = graph;
        atomSet.addAll(AtomicFactory.createAtomSet(pattern, this));
        inferTypes();
    }

    public ReasonerQueryImpl(ReasonerQueryImpl q) {
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

    @Override
    public String toString() {
        return getPattern().toString();
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
        return selectAtoms().size() == 1;
    }

    /**
     * @return atom that should be prioritised for resolution
     */
    Atom getTopAtom() {
        //TODO redo based on priority function
        Set<Atom> atoms = selectAtoms();

        //favour atoms with substitutions
        List<Atom> subbedAtoms = atoms.stream()
                .filter(Atom::hasSubstitution)
                .sorted(Comparator.comparing(at -> at.getApplicableRules().size()))
                .collect(Collectors.toList());
        if (!subbedAtoms.isEmpty()) return subbedAtoms.iterator().next();

        //favour resources with value predicates
        Set<Resource> resources = atoms.stream()
                .filter(Atom::isResource).map(at -> (Resource) at)
                .filter(r -> !r.getMultiPredicate().isEmpty())
                .collect(Collectors.toSet());
        if (!resources.isEmpty()) return resources.iterator().next();

        //favour non-resolvable relations
        Set<Atom> relations = atoms.stream()
                .filter(Atom::isRelation)
                .filter(at -> !at.isRuleResolvable())
                .collect(Collectors.toSet());
        if (!relations.isEmpty()) return relations.iterator().next();

        //favour resolvable relations
        Set<Atom> resolvableRelations = atoms.stream()
                .filter(Atom::isRelation)
                .filter(Atom::isRuleResolvable)
                .collect(Collectors.toSet());
        if (!resolvableRelations.isEmpty()) return resolvableRelations.iterator().next();

        return atoms.stream().findFirst().orElse(null);
    }

    /**
     * @return atom set constituting this query
     */
    @Override
    public Set<Atomic> getAtoms() {
        return Sets.newHashSet(atomSet);
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
    public Set<VarName> getVarNames() {
        Set<VarName> vars = new HashSet<>();
        atomSet.forEach(atom -> vars.addAll(atom.getVarNames()));
        return vars;
    }

    /**
     * @param atom in question
     * @return true if query contains an equivalent atom
     */
    public boolean containsEquivalentAtom(Atomic atom) {
        return !getEquivalentAtoms(atom).isEmpty();
    }

    Set<Atomic> getEquivalentAtoms(Atomic atom) {
        return atomSet.stream().filter(at -> at.isEquivalent(atom)).collect(Collectors.toSet());
    }

    private void exchangeRelVarNames(VarName from, VarName to) {
        unify(to, VarName.of("temp"));
        unify(from, to);
        unify(VarName.of("temp"), from);
    }

    @Override
    public Unifier getUnifier(ReasonerQuery parent) {
        throw new IllegalStateException("Attempted to obtain unifiers on non-atomic queries.");
    }

    /**
     * change each variable occurrence in the query (apply unifier [from/to])
     *
     * @param from variable name to be changed
     * @param to   new variable name
     */
    @Override
    public void unify(VarName from, VarName to) {
        Set<Atomic> toRemove = new HashSet<>();
        Set<Atomic> toAdd = new HashSet<>();

        atomSet.stream().filter(atom -> atom.getVarNames().contains(from)).forEach(toRemove::add);
        toRemove.forEach(atom -> toAdd.add(AtomicFactory.create(atom, this)));
        toRemove.forEach(this::removeAtomic);
        toAdd.forEach(atom -> atom.unify(new UnifierImpl(ImmutableMap.of(from, to))));
        toAdd.forEach(this::addAtomic);
    }

    /**
     * change each variable occurrence according to provided mappings (apply unifiers {[from, to]_i})
     * @param unifier (variable mappings) to be applied
     */
    @Override
    public void unify(Unifier unifier) {
        if (unifier.size() == 0) return;
        Unifier mappings = new UnifierImpl(unifier);
        Unifier appliedMappings = new UnifierImpl();
        //do bidirectional mappings if any
        for (Map.Entry<VarName, VarName> mapping: mappings.getMappings()) {
            VarName varToReplace = mapping.getKey();
            VarName replacementVar = mapping.getValue();
            //bidirectional mapping
            if (!replacementVar.equals(appliedMappings.get(varToReplace)) && varToReplace.equals(mappings.get(replacementVar))) {
                exchangeRelVarNames(varToReplace, replacementVar);
                appliedMappings.addMapping(varToReplace, replacementVar);
                appliedMappings.addMapping(replacementVar, varToReplace);
            }
        }
        mappings.getMappings().removeIf(e ->
                appliedMappings.containsKey(e.getKey()) && appliedMappings.get(e.getKey()).equals(e.getValue()));

        Set<Atomic> toRemove = new HashSet<>();
        Set<Atomic> toAdd = new HashSet<>();

        atomSet.stream()
                .filter(atom -> {
                    Set<VarName> keyIntersection = atom.getVarNames();
                    Set<VarName> valIntersection = atom.getVarNames();
                    keyIntersection.retainAll(mappings.keySet());
                    valIntersection.retainAll(mappings.values());
                    return (!keyIntersection.isEmpty() || !valIntersection.isEmpty());
                })
                .forEach(toRemove::add);
        toRemove.forEach(atom -> toAdd.add(AtomicFactory.create(atom, this)));
        toRemove.forEach(this::removeAtomic);
        toAdd.forEach(atom -> atom.unify(mappings));
        toAdd.forEach(this::addAtomic);

        //NB:captures not resolved in place as resolution in-place alters respective atom hash
        mappings.merge(resolveCaptures());
    }

    /**
     * finds captured variable occurrences in a query and replaces them with fresh variables
     *
     * @return new mappings resulting from capture resolution
     */
    private Unifier resolveCaptures() {
        Unifier newMappings = new UnifierImpl();
        //find and resolve captures
        // TODO: This could cause bugs if a user has a variable including the word "capture"
        getVarNames().stream().filter(Utility::isCaptured)
                .forEach(cap -> {
                    VarName old = uncapture(cap);
                    VarName fresh = VarName.anon();
                    unify(cap, fresh);
                    newMappings.addMapping(old, fresh);
                });
        return newMappings;
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
    public Map<VarName, Type> getVarTypeMap() {
        Map<VarName, Type> map = new HashMap<>();
        getTypeConstraints().stream()
                .filter(at -> Objects.nonNull(at.getType()))
                .forEach(atom -> map.putIfAbsent(atom.getVarName(), atom.getType()));
        return map;
    }

    /**
     * @param var variable name
     * @return id predicate for the specified var name if any
     */
    public IdPredicate getIdPredicate(VarName var) {
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
     * remove given atom together with its disjoint neigbours (atoms it is connected to)
     * @param atom to be removed
     * @return modified query
     */
    ReasonerQueryImpl removeAtom(Atom atom){
        removeAtomic(atom);
        atom.getNonSelectableConstraints().stream()
                .filter(at -> findNextJoinable(atom) == null)
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

    private Atom findNextJoinable(Set<Atom> atoms, Set<VarName> vars){
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
        Set<Atom> atoms = new HashSet<>(atomSet).stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .collect(Collectors.toSet());
        if (atoms.size() == 1) return atoms;

        //pass relations or rule-resolvable types and resources
        Set<Atom> atomsToSelect = atoms.stream()
                .filter(Atomic::isSelectable)
                .collect(Collectors.toSet());

        Set<Atom> orderedSelection = new LinkedHashSet<>();

        Atom atom = findFirstJoinable(atomsToSelect);
        Set<VarName> joinedVars = new HashSet<>();
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

    private Answer getSubstitution(){
        Set<IdPredicate> predicates = this.getTypeConstraints().stream()
                .map(TypeAtom::getPredicate)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        predicates.addAll(getIdPredicates());

        return new QueryAnswer(predicates.stream()
                .collect(Collectors.toMap(IdPredicate::getVarName, p -> graph().getConcept(p.getPredicate())))
        );
    }

    ReasonerQuery addSubstitution(Answer sub){
        Set<VarName> varNames = getVarNames();

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
            if (atom.requiresMaterialisation() && atom.isRuleResolvable()) return true;
            for (InferenceRule rule : atom.getApplicableRules())
                if (rule.requiresMaterialisation()){
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
        Set<VarName> joinedVars = childAtomicQuery.getVarNames();
        while(qit.hasNext()){
            childAtomicQuery = qit.next();
            Set<VarName> joinVars = Sets.intersection(joinedVars, childAtomicQuery.getVarNames());
            Stream<Answer> localSubs = childAtomicQuery.answerStream(subGoals, cache, dCache, materialise, explanation, false);
            join = joinWithInverse(
                    join,
                    localSubs,
                    cache.getInverseAnswerMap(childAtomicQuery, joinVars),
                    ImmutableSet.copyOf(joinVars),
                    explanation);
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
            Set<VarName> joinedVars = qi.getVarNames();
            for(ReasonerAtomicQuery qj : queries){
                if ( qj != qi ){
                    Set<VarName> joinVars = Sets.intersection(joinedVars, qj.getVarNames());
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
        if (!this.isRuleResolvable()) {
            return this.getMatchQuery().admin().streamWithVarNames().map(QueryAnswer::new);
        }
        if (materialise || requiresMaterialisation()) {
            return resolve(materialise, explanation, new LazyQueryCache<>(explanation), new LazyQueryCache<>(explanation));
        } else {
            return new QueryAnswerIterator().hasStream();
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
        Set<VarName> joinedVars = atomicQuery.getVarNames();
        while (atIt.hasNext()) {
            atomicQuery = new ReasonerAtomicQuery(atIt.next());
            Stream<Answer> subAnswerStream = atomicQuery.resolve(materialise, explanation, cache, dCache);
            Set<VarName> joinVars = Sets.intersection(joinedVars, atomicQuery.getVarNames());
            answerStream = join(answerStream, subAnswerStream, ImmutableSet.copyOf(joinVars), explanation);
            joinedVars.addAll(atomicQuery.getVarNames());
        }

        Set<NotEquals> filters = this.getFilters();
        Set<VarName> vars = this.getVarNames();
        return answerStream
                .filter(a -> nonEqualsFilter(a, filters))
                .flatMap(a -> varFilterFunction.apply(a, vars));
    }

    public ReasonerQueryIterator iterator(Answer sub, Set<ReasonerAtomicQuery> subGoals, QueryCache<ReasonerAtomicQuery> cache){
        return new ReasonerQueryImplIterator(this, sub, subGoals, cache);
    }

    private class QueryAnswerIterator extends ReasonerQueryIterator {

        private int iter = 0;
        private long oldAns = 0;
        Set<Answer> answers = new HashSet<>();

        private final QueryCache<ReasonerAtomicQuery> cache;
        private Iterator<Answer> answerIterator;

        QueryAnswerIterator(){ this(new QueryCache<>());}
        QueryAnswerIterator(QueryCache<ReasonerAtomicQuery> qc){
            this.cache = qc;
            this.answerIterator = new ReasonerQueryImplIterator(ReasonerQueryImpl.this, new QueryAnswer(), new HashSet<>(), cache);
        }

        /**
         * check whether answers available, if answers not fully computed compute more answers
         * @return true if answers available
         */
        @Override
        public boolean hasNext() {
            if (answerIterator.hasNext()) return true;
                //iter finished
            else {
                long dAns = answers.size() - oldAns;
                if (dAns != 0 || iter == 0) {
                    LOG.debug("iter: " + iter + " answers: " + answers.size() + " dAns = " + dAns);
                    iter++;
                    answerIterator = new ReasonerQueryImplIterator(ReasonerQueryImpl.this, new QueryAnswer(), new HashSet<>(), cache);
                    oldAns = answers.size();
                    return answerIterator.hasNext();
                }
                else return false;
            }
        }

        /**
         * @return single answer to the query
         */
        @Override
        public Answer next() {
            Answer ans = answerIterator.next();
            answers.add(ans);
            return ans;
        }

    }
}
