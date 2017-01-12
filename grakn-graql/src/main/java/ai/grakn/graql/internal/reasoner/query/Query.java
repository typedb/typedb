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
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.Utility;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.Atomic;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.NotEquals;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.reasoner.Utility.isCaptured;
import static ai.grakn.graql.internal.reasoner.Utility.uncapture;

/**
 *
 * <p>
 * Base reasoner query providing resolution and atom handling facilities for conjunctive graql queries.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class Query {

    private final GraknGraph graph;
    private final Set<Atomic> atomSet = new HashSet<>();
    private final Set<VarName> selectVars;

    public Query(MatchQuery query, GraknGraph graph) {
        this.graph = graph;
        this.selectVars = Sets.newHashSet(query.admin().getSelectedNames());
        atomSet.addAll(AtomicFactory.createAtomSet(query.admin().getPattern(), this, graph));
        inferTypes();
    }

    public Query(String query, GraknGraph graph) {
        this(graph.graql().infer(false).<MatchQuery>parse(query), graph);
    }

    public Query(Query q) {
        this.graph = q.graph;
        this.selectVars = q.getSelectedNames();
        q.getAtoms().forEach(at -> addAtom(AtomicFactory.create(at, this)));
        inferTypes();
    }

    protected Query(Atom atom, Set<VarName> vars) {
        if (atom.getParentQuery() == null)
            throw new IllegalArgumentException(ErrorMessage.PARENT_MISSING.getMessage(atom.toString()));
        this.graph = atom.getParentQuery().graph;
        this.selectVars = atom.getSelectedNames();
        selectVars.addAll(vars);
        addAtom(AtomicFactory.create(atom, this));
        addAtomConstraints(atom);
        selectVars.retainAll(getVarSet());
        inferTypes();
    }

    //alpha-equivalence equality
    @Override
    public boolean equals(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        Query a2 = (Query) obj;
        return this.isEquivalent(a2);
    }

    @Override
    public int hashCode(){
        int hashCode = 1;
        SortedSet<Integer> hashes = new TreeSet<>();
        atomSet.forEach(atom -> hashes.add(atom.equivalenceHashCode()));
        for (Integer hash : hashes) hashCode = hashCode * 37 + hash;
        return hashCode;
    }

    @Override
    public String toString() { return getMatchQuery().toString();}

    private void inferTypes(){
        getAtoms().stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .forEach(Atom::inferTypes);
    }

    public Set<VarName> getSelectedNames() { return Sets.newHashSet(selectVars);}

    /**
     * append to select variables
     * @param vars variables to append
     * @return appended query
     */
    public void selectAppend(Set<VarName> vars){
        selectVars.addAll(vars);
    }

    public GraknGraph graph(){ return graph;}

    public Conjunction<PatternAdmin> getPattern() {
        Set<PatternAdmin> patterns = new HashSet<>();
        atomSet.stream().map(Atomic::getCombinedPattern).forEach(patterns::add);
        return Patterns.conjunction(patterns);
    }

    /**
     * @return true if any of the atoms constituting the query can be resolved through a rule
     */
    public boolean isRuleResolvable(){
        boolean ruleResolvable = false;
        Iterator<Atomic> it = atomSet.iterator();
        while(it.hasNext() && !ruleResolvable)
            ruleResolvable = it.next().isRuleResolvable();
        return ruleResolvable;
    }

    public QueryAnswers getAnswers(){ throw new IllegalStateException(ErrorMessage.ANSWER_ERROR.getMessage());}
    public QueryAnswers getNewAnswers(){ throw new IllegalStateException(ErrorMessage.ANSWER_ERROR.getMessage());}

    /**
     * resolve the query by performing either a db or memory lookup, depending on which is more appropriate
     * @param cache container of already performed query resolutions
     */
    public void lookup(QueryCache cache){ throw new IllegalStateException(ErrorMessage.ANSWER_ERROR.getMessage());}

    /**
     * resolve the query by performing a db lookup
     */
    public void DBlookup(){ throw new IllegalStateException(ErrorMessage.ANSWER_ERROR.getMessage());}

    /**
     * resolve the query by performing a memory (cache) lookup
     * @param cache container of already performed query resolutions
     */
    public void memoryLookup(QueryCache cache){
        throw new IllegalStateException(ErrorMessage.ANSWER_ERROR.getMessage());
    }

    /**
     * propagate answers to relation resolutions in the cache
     * @param cache container of already performed query resolutions
     */
    public void propagateAnswers(QueryCache cache){
        throw new IllegalStateException(ErrorMessage.ANSWER_ERROR.getMessage());
    }

    /**
     * @return atom set constituting this query
     */
    public Set<Atomic> getAtoms() { return Sets.newHashSet(atomSet);}

    /**
     * @return set of id predicates contained in this query
     */
    public Set<IdPredicate> getIdPredicates(){
        return getAtoms().stream()
                .filter(Atomic::isPredicate)
                .map(at -> (Predicate) at)
                .filter(Predicate::isIdPredicate)
                .map(predicate -> (IdPredicate) predicate)
                .collect(Collectors.toSet());
    }

    /**
     * @return set of value predicates contained in this query
     */
    public Set<Predicate> getValuePredicates(){
        return getAtoms().stream()
                .filter(Atomic::isPredicate)
                .map(at -> (Predicate) at)
                .filter(Predicate::isValuePredicate)
                .collect(Collectors.toSet());
    }

    /**
     * @return set of resource atoms contained in this query
     */
    public Set<Atom> getResources(){
        return getAtoms().stream()
                .filter(Atomic::isAtom)
                .map(at -> (Atom) at)
                .filter(Atom::isResource)
                .collect(Collectors.toSet());
    }

    /**
     * @return set of atoms constituting constraints (by means of types) for this atom
     */
    public Set<Atom> getTypeConstraints(){
        return getAtoms().stream()
                .filter(Atomic::isAtom)
                .map(at -> (Atom) at)
                .filter(Atom::isType)
                .collect(Collectors.toSet());
    }

    /**
     * @return set of filter atoms (currently only NotEquals) contained in this query
     */
    public Set<NotEquals> getFilters(){
        return getAtoms().stream()
                .filter(at -> at.getClass() == NotEquals.class)
                .map(at -> (NotEquals) at)
                .collect(Collectors.toSet());
    }

    /**
     * @return set of variables appearing in this query
     */
    public Set<VarName> getVarSet() {
        Set<VarName> vars = new HashSet<>();
        atomSet.forEach(atom -> vars.addAll(atom.getVarNames()));
        return vars;
    }

    /**
     * @param atom in question
     * @return true if atom is contained in the query
     */
    public boolean containsAtom(Atomic atom){ return atomSet.contains(atom);}

    /**
     * @param atom in question
     * @return true if query contains an equivalent atom
     */
    public boolean containsEquivalentAtom(Atomic atom){
        boolean isContained = false;
        Iterator<Atomic> it = atomSet.iterator();
        while( it.hasNext() && !isContained) {
            Atomic at = it.next();
            isContained = atom.isEquivalent(at);
        }
        return isContained;
    }

    private void updateSelectedVars(Map<VarName, VarName> mappings) {
        Set<VarName> toRemove = new HashSet<>();
        Set<VarName> toAdd = new HashSet<>();
        mappings.forEach( (from, to) -> {
                    if (selectVars.contains(from)) {
                        toRemove.add(from);
                        toAdd.add(to);
                    }
                });
        toRemove.forEach(selectVars::remove);
        toAdd.forEach(selectVars::add);
    }

    private void exchangeRelVarNames(VarName from, VarName to){
        unify(to, Patterns.varName("temp"));
        unify(from, to);
        unify(Patterns.varName("temp"), from);
    }

    /**
     * change each variable occurrence in the query (apply unifier [from/to])
     * @param from variable name to be changed
     * @param to new variable name
     */
    public void unify(VarName from, VarName to) {
        Set<Atomic> toRemove = new HashSet<>();
        Set<Atomic> toAdd = new HashSet<>();

        atomSet.stream().filter(atom -> atom.getVarNames().contains(from)).forEach(toRemove::add);
        toRemove.forEach(atom -> toAdd.add(AtomicFactory.create(atom, this)));
        toRemove.forEach(this::removeAtom);
        toAdd.forEach(atom -> atom.unify(ImmutableMap.of(from, to)));
        toAdd.forEach(this::addAtom);

        Map<VarName, VarName> mapping = new HashMap<>();
        mapping.put(from, to);
        updateSelectedVars(mapping);
    }

    /**
     * change each variable occurrence according to provided mappings (apply unifiers {[from, to]_i})
     * @param unifiers contain unifiers (variable mappings) to be applied
     */
    public void unify(Map<VarName, VarName> unifiers) {
        if (unifiers.size() == 0) return;
        Map<VarName, VarName> mappings = new HashMap<>(unifiers);
        Map<VarName, VarName> appliedMappings = new HashMap<>();
        //do bidirectional mappings if any
        for (Map.Entry<VarName, VarName> mapping: mappings.entrySet()) {
            VarName varToReplace = mapping.getKey();
            VarName replacementVar = mapping.getValue();
            if(!appliedMappings.containsKey(varToReplace) || !appliedMappings.get(varToReplace).equals(replacementVar)) {
                //bidirectional mapping
                if (mappings.containsKey(replacementVar) && mappings.get(replacementVar).equals(varToReplace)) {
                    exchangeRelVarNames(varToReplace, replacementVar);
                    appliedMappings.put(varToReplace, replacementVar);
                    appliedMappings.put(replacementVar, varToReplace);
                }
            }
        }
        mappings.entrySet().removeIf(e ->
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
        toRemove.forEach(this::removeAtom);
        toAdd.forEach(atom -> atom.unify(mappings));
        toAdd.forEach(this::addAtom);

        mappings.putAll(resolveCaptures());
        updateSelectedVars(mappings);
    }

    /**
     * finds captured variable occurrences in a query and replaces them with fresh variables
     * @return new mappings resulting from capture resolution
     */
    private Map<VarName, VarName> resolveCaptures() {
        Map<VarName, VarName> newMappings = new HashMap<>();
        //find captures
        Set<VarName> captures = new HashSet<>();
        getVarSet().forEach(v -> {
            // TODO: This could cause bugs if a user has a variable including the word "capture"
            if (isCaptured(v)) captures.add(v);
        });

        captures.forEach(cap -> {
            VarName old = uncapture(cap);
            VarName fresh = Utility.createFreshVariable(getVarSet(), old);
            unify(cap, fresh);
            newMappings.put(old, fresh);
        });
        return newMappings;
    }

    /**
     * @return corresponding MatchQuery
     */
    public MatchQuery getMatchQuery() {
        if (selectVars.isEmpty())
            return graph.graql().infer(false).match(getPattern());
        else
            return graph.graql().infer(false).match(getPattern()).select(selectVars);
    }

    /**
     * @return map of variable name - type pairs
     */
    public Map<VarName, Type> getVarTypeMap() {
        Map<VarName, Type> map = new HashMap<>();
        getTypeConstraints().forEach(atom -> map.putIfAbsent(atom.getVarName(), atom.getType()));
        return map;
    }

    /**
     * @param var variable name
     * @return id predicate for the specified var name if any
     */
    public IdPredicate getIdPredicate(VarName var) {
        //direct
        Set<IdPredicate> relevantSubs = getIdPredicates().stream()
                .filter(sub -> sub.getVarName().equals(var))
                .collect(Collectors.toSet());
        //indirect
        getTypeConstraints().stream()
                .filter(type -> type.getVarName().equals(var))
                .forEach(type -> type.getPredicates().stream().findFirst().
                        ifPresent(predicate -> relevantSubs.add((IdPredicate) predicate)));
        return relevantSubs.isEmpty() ? null : relevantSubs.iterator().next();
    }

    /**
     * @param atom to be added
     * @return true if the atom set did not already contain the specified atom
     */
    public boolean addAtom(Atomic atom) {
        if(atomSet.add(atom)) {
            atom.setParentQuery(this);
            return true;
        }
        else return false;
    }


    /**
     * @param atom to be removed
     * @return true if the atom set contained the specified atom
     */
    public boolean removeAtom(Atomic atom) {return atomSet.remove(atom);}

    private void addAtomConstraints(Atom atom){
        addAtomConstraints(atom.getPredicates());
        Set<Atom> types = atom.getTypeConstraints().stream()
                .filter(at -> !at.isSelectable())
                .filter(at -> !at.isRuleResolvable())
                .collect(Collectors.toSet());
        addAtomConstraints(types);
    }

    /**
     * adds a set of constraints (types, predicates) to the atom set
     * @param cstrs set of constraints
     */
    public void addAtomConstraints(Set<? extends Atomic> cstrs){
        cstrs.forEach(con -> addAtom(AtomicFactory.create(con, this)));
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
        Set<Atom> selectedAtoms = atoms.stream()
                .filter(atom -> (atom.isSelectable() || atom.isRuleResolvable()))
                .collect(Collectors.toSet());

        //order by variables
        Set<Atom> orderedSelection = new LinkedHashSet<>();
        getVarSet().forEach(var -> orderedSelection.addAll(selectedAtoms.stream()
                .filter(atom -> atom.containsVar(var))
                .collect(Collectors.toSet())));

        if (orderedSelection.isEmpty())
            throw new IllegalStateException(ErrorMessage.NO_ATOMS_SELECTED.getMessage(this.toString()));
        return orderedSelection;
    }

    /**
     * @param q query to be compared with
     * @return true if two queries are alpha-equivalent
     */
    public boolean isEquivalent(Query q) {
        boolean equivalent = true;
        Set<Atom> atoms = atomSet.stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .collect(Collectors.toSet());
        if(atoms.size() != q.getAtoms().stream().filter(Atomic::isAtom).count()) return false;
        Iterator<Atom> it = atoms.iterator();
        while (it.hasNext() && equivalent) {
            Atom atom = it.next();
            equivalent = q.containsEquivalentAtom(atom);
        }
        return equivalent;
    }

    /**
     * resolves the query
     * @param materialise materialisation flag
     * @return stream of answers
     */
    public Stream<Map<VarName, Concept>> resolve(boolean materialise) {
        throw new IllegalStateException(ErrorMessage.ANSWER_ERROR.getMessage());
    }
}
