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
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.query.match.MatchOrder;
import ai.grakn.graql.internal.reasoner.Utility;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.Atomic;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.Binary;
import ai.grakn.graql.internal.reasoner.atom.TypeAtom;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.Sets;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Type;
import ai.grakn.graql.Graql;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.internal.query.match.MatchQueryInternal;
import ai.grakn.graql.internal.reasoner.atom.Predicate;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Query implements MatchQueryInternal {

    protected final GraknGraph graph;
    private final Set<Atomic> atomSet = new HashSet<>();
    private final Set<String> selectVars;

    public Query(MatchQuery query, GraknGraph graph) {
        this.graph = graph;
        this.selectVars = Sets.newHashSet(query.admin().getSelectedNames());
        atomSet.addAll(AtomicFactory.createAtomSet(query.admin().getPattern(), this, graph));
    }

    public Query(String query, GraknGraph graph) {
        this(graph.graql().<MatchQuery>parse(query), graph);
    }

    public Query(Query q) {
        this.graph = q.getGraph().orElse(null);
        this.selectVars = q.getSelectedNames();
        q.getAtoms().forEach(at -> addAtom(AtomicFactory.create(at, this)));
    }

    protected Query(Atom atom, Set<String> vars) {
        if (atom.getParentQuery() == null)
            throw new IllegalArgumentException(ErrorMessage.PARENT_MISSING.getMessage(atom.toString()));
        this.graph = atom.getParentQuery().getGraph().orElse(null);
        this.selectVars = atom.getSelectedNames();
        selectVars.addAll(vars);
        addAtom(AtomicFactory.create(atom, this));
        addAtomConstraints(atom);
        selectVars.retainAll(getVarSet());
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
        Iterator<Integer> it = hashes.iterator();
        while(it.hasNext()){
            Integer hash = it.next();
            hashCode = hashCode * 37 + hash;
        }
        return hashCode;
    }

    @Override
    public String toString() { return getMatchQuery().toString();}

    @Override
    public Set<Type> getTypes(GraknGraph graph){ return getMatchQuery().admin().getTypes(graph);}

    @Override
    public Set<Type> getTypes() { return getMatchQuery().admin().getTypes(); }

    @Override
    public Set<String> getSelectedNames() { return Sets.newHashSet(selectVars);}

    @Override
    public MatchQuery select(Set<String> vars){ return this;}

    @Override
    public Stream<Map<String, Concept>> stream(Optional<GraknGraph> graph, Optional<MatchOrder> order) {
        return getMatchQuery().stream();
    }

    @Override
    public Optional<GraknGraph> getGraph(){ return Optional.of(graph);}
    public GraknGraph graph(){ return graph;}

    @Override
    public Conjunction<PatternAdmin> getPattern() {
        Set<PatternAdmin> patterns = new HashSet<>();
        atomSet.stream().map(Atomic::getPattern).forEach(patterns::add);
        return Patterns.conjunction(patterns);
    }

    @Override
    public List<Map<String, Concept>> execute() { return getMatchQuery().execute();}

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
    public void DBlookup(){ throw new IllegalStateException(ErrorMessage.ANSWER_ERROR.getMessage());}
    public void memoryLookup(Map<AtomicQuery, AtomicQuery> matAnswers){
        throw new IllegalStateException(ErrorMessage.ANSWER_ERROR.getMessage());
    }
    public void propagateAnswers(Map<AtomicQuery, AtomicQuery> matAnswers){
        throw new IllegalStateException(ErrorMessage.ANSWER_ERROR.getMessage());
    }

    public Set<Atomic> getAtoms() { return Sets.newHashSet(atomSet);}
    public Set<Predicate> getIdPredicates(){
        return getAtoms().stream()
                .filter(Atomic::isPredicate)
                .map(at -> (Predicate) at)
                .filter(Predicate::isIdPredicate)
                .collect(Collectors.toSet());
    }

    public Set<Predicate> getValuePredicates(){
        return getAtoms().stream()
                .filter(Atomic::isPredicate)
                .map(at -> (Predicate) at)
                .filter(Predicate::isValuePredicate)
                .collect(Collectors.toSet());
    }

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

    public Set<String> getVarSet() {
        Set<String> vars = new HashSet<>();
        atomSet.forEach(atom -> vars.addAll(atom.getVarNames()));
        return vars;
    }

    public boolean containsAtom(Atomic atom){ return atomSet.contains(atom);}
    public boolean containsEquivalentAtom(Atomic atom){
        boolean isContained = false;
        Iterator<Atomic> it = atomSet.iterator();
        while( it.hasNext() && !isContained) {
            Atomic at = it.next();
            isContained = atom.isEquivalent(at);
        }
        return isContained;
    }

    private void updateSelectedVars(Map<String, String> mappings) {
        Set<String> toRemove = new HashSet<>();
        Set<String> toAdd = new HashSet<>();
        mappings.forEach( (from, to) -> {
                    if (selectVars.contains(from)) {
                        toRemove.add(from);
                        toAdd.add(to);
                    }
                });
        toRemove.forEach(selectVars::remove);
        toAdd.forEach(selectVars::add);
    }

    private void exchangeRelVarNames(String from, String to){
        unify(to, "temp");
        unify(from, to);
        unify("temp", from);
    }

    /**
     * change each variable occurrence in the query (apply unifier [from/to])
     * @param from variable name to be changed
     * @param to new variable name
     */
    public void unify(String from, String to) {
        Set<Atomic> toRemove = new HashSet<>();
        Set<Atomic> toAdd = new HashSet<>();

        atomSet.stream().filter(atom -> atom.getVarNames().contains(from)).forEach(toRemove::add);
        toRemove.forEach(atom -> toAdd.add(AtomicFactory.create(atom, this)));
        toRemove.forEach(this::removeAtom);
        toAdd.forEach(atom -> atom.unify(from, to));
        toAdd.forEach(this::addAtom);

        Map<String, String> mapping = new HashMap<>();
        mapping.put(from, to);
        updateSelectedVars(mapping);
    }

    /**
     * change each variable occurrence according to provided mappings (apply unifiers {[from, to]_i})
     * @param unifiers contain unifiers (variable mappings) to be applied
     */
    public void unify(Map<String, String> unifiers) {
        if (unifiers.size() == 0) return;
        Map<String, String> mappings = new HashMap<>(unifiers);
        Map<String, String> appliedMappings = new HashMap<>();
        //do bidirectional mappings if any
        for (Map.Entry<String, String> mapping: mappings.entrySet()) {
            String varToReplace = mapping.getKey();
            String replacementVar = mapping.getValue();

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
                    Set<String> keyIntersection = atom.getVarNames();
                    Set<String> valIntersection = atom.getVarNames();
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
    private Map<String, String> resolveCaptures() {
        Map<String, String> newMappings = new HashMap<>();
        //find captures
        Set<String> captures = new HashSet<>();
        getVarSet().forEach(v -> {
            if (v.contains("capture")) captures.add(v);
        });

        captures.forEach(cap -> {
            String old = cap.replace("captured->", "");
            String fresh = Utility.createFreshVariable(getVarSet(), old);
            unify(cap, fresh);
            newMappings.put(old, fresh);
        });
        return newMappings;
    }

    public MatchQuery getMatchQuery() {
        if (selectVars.isEmpty())
            return Graql.match(getPattern()).withGraph(graph);
        else
            return Graql.match(getPattern()).select(selectVars).withGraph(graph);
    }

    public Map<String, Type> getVarTypeMap() {
        Map<String, Type> map = new HashMap<>();
        getTypeConstraints().forEach(atom -> map.putIfAbsent(atom.getVarName(), atom.getType()));
        return map;
    }

    public Predicate getIdPredicate(String var) {
        //direct
        Set<Predicate> relevantSubs = getIdPredicates().stream()
                .filter(sub -> sub.getVarName().equals(var))
                .collect(Collectors.toSet());
        //indirect
        getTypeConstraints().stream()
                .filter(type -> type.getVarName().equals(var))
                .forEach(type -> type.getPredicates().stream().findFirst().ifPresent(relevantSubs::add));
        return relevantSubs.isEmpty() ? null : relevantSubs.iterator().next();
    }

    //TODO should return predicate
    public String getValuePredicate(String var){
        Set<Predicate> relevantVPs = getValuePredicates().stream()
                .filter(vp -> vp.getVarName().equals(var))
                .collect(Collectors.toSet());
        return relevantVPs.isEmpty()? "" : relevantVPs.iterator().next().getPredicateValue();
    }

    public boolean addAtom(Atomic atom) {
        if(atomSet.add(atom)) {
            atom.setParentQuery(this);
            return true;
        }
        else return false;
    }

    public boolean removeAtom(Atomic atom) {return atomSet.remove(atom);}

    public void addAtomConstraints(Set<? extends Atomic> cstrs){
        cstrs.forEach(con -> addAtom(AtomicFactory.create(con, this)));
    }

    private void addAtomConstraints(Atom atom){
        addAtomConstraints(atom.getPredicates());
        Set<Atom> types = atom.getTypeConstraints().stream()
                .filter(at -> !at.isSelectable())
                .filter(at -> !at.isRuleResolvable())
                .collect(Collectors.toSet());
        addAtomConstraints(types);
        //id predicates from types
        addAtomConstraints(types.stream()
                .map(type -> (TypeAtom) type)
                .filter(type -> type.getPredicate() != null)
                .map(Binary::getPredicate)
                .collect(Collectors.toSet()));
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
}
