/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.reasoner.query;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Type;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.admin.Conjunction;
import io.mindmaps.graql.internal.query.match.MatchOrder;
import io.mindmaps.graql.internal.query.match.MatchQueryInternal;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.graql.admin.PatternAdmin;
import io.mindmaps.graql.internal.pattern.Patterns;
import io.mindmaps.graql.internal.reasoner.predicate.Atomic;
import io.mindmaps.graql.internal.reasoner.predicate.AtomicFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.mindmaps.graql.internal.reasoner.Utility.createFreshVariable;

public class Query implements MatchQueryInternal {

    protected final MindmapsGraph graph;
    protected final Set<Atomic> atomSet;

    private final Conjunction<PatternAdmin> pattern;
    private final Set<String> selectVars;

    private final Map<Type, Set<Atomic>> typeAtomMap;

    public Query(String query, MindmapsGraph graph) {
        this.graph = graph;
        MatchQuery matchQuery = Graql.withGraph(graph).parseMatch(query);
        this.pattern = matchQuery.admin().getPattern();
        this.selectVars = Sets.newHashSet(matchQuery.admin().getSelectedNames());

        this.atomSet = AtomicFactory.createAtomSet(pattern, this);
        this.typeAtomMap = getTypeAtomMap(atomSet);
    }

    public Query(MatchQuery query, MindmapsGraph graph) {
        this.graph = graph;

        this.pattern = query.admin().getPattern();
        this.selectVars = Sets.newHashSet(query.admin().getSelectedNames());

        this.atomSet = AtomicFactory.createAtomSet(pattern, this);
        this.typeAtomMap = getTypeAtomMap(atomSet);
    }

    public Query(Query q) {
        this(q.toString(), q.graph);
    }

    protected Query(Atomic atom) {
        if (atom.getParentQuery() == null)
            throw new IllegalArgumentException(ErrorMessage.PARENT_MISSING.getMessage(atom.toString()));
        this.graph = atom.getParentQuery().getGraph().orElse(null);
        this.pattern = Patterns.conjunction(Sets.newHashSet());
        this.selectVars = Sets.newHashSet(atom.getMatchQuery(graph).admin().getSelectedNames());

        atomSet = new HashSet<>();
        addAtom(atom);
        addAtomConstraints(atom.getSubstitutions());
        if(atom.isRelation() || atom.isResource())
            addAtomConstraints(atom.getTypeConstraints()
                                    .stream().filter(at -> !at.isRuleResolvable())
                                    .collect(Collectors.toSet()));

        this.typeAtomMap = getTypeAtomMap(atomSet);
    }

    //alpha-equivalence equality
    @Override
    public boolean equals(Object obj){
        if (!(obj instanceof Query)) return false;
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
    public Set<Type> getTypes(MindmapsGraph graph){ return getMatchQuery().admin().getTypes(graph);}

    @Override
    public Set<Type> getTypes() { return getMatchQuery().admin().getTypes(); }

    @Override
    public Set<String> getSelectedNames() { return selectVars;}

    @Override
    public Stream<Map<String, Concept>> stream(Optional<MindmapsGraph> graph, Optional<MatchOrder> order) {
        return getMatchQuery().stream();
    }

    @Override
    public Optional<MindmapsGraph> getGraph(){ return Optional.of(graph);}

    @Override
    public Conjunction<PatternAdmin> getPattern(){ return pattern;}

    public boolean isRuleResolvable(){
        boolean ruleResolvable = false;
        Iterator<Atomic> it = atomSet.iterator();
        while(it.hasNext() && !ruleResolvable)
            ruleResolvable = it.next().isRuleResolvable();

        return ruleResolvable;
    }

    public QueryAnswers getAnswers(){ throw new IllegalStateException(ErrorMessage.ANSWER_ERROR.getMessage());}
    public void DBlookup(){ throw new IllegalStateException(ErrorMessage.ANSWER_ERROR.getMessage());}
    public void memoryLookup(Map<AtomicQuery, QueryAnswers> matAnswers){
        throw new IllegalStateException(ErrorMessage.ANSWER_ERROR.getMessage());
    }
    public void propagateAnswers(Map<AtomicQuery, QueryAnswers> matAnswers){
        throw new IllegalStateException(ErrorMessage.ANSWER_ERROR.getMessage());
    }

    public Set<Atomic> getAtoms() { return new HashSet<>(atomSet);}
    public Set<Atomic> getAtomsWithType(Type type) {
        return typeAtomMap.get(type);
    }
    public Set<Atomic> getSubstitutions(){
        return getAtoms().stream().filter(Atomic::isSubstitution).collect(Collectors.toSet());
    }
    public Set<Atomic> getTypeConstraints(){
        return getAtoms().stream().filter(atom -> atom.isUnary() && !atom.isResource()).collect(Collectors.toSet());
    }

    public Set<String> getVarSet() {
        Set<String> vars = new HashSet<>();
        atomSet.forEach(atom -> vars.addAll(atom.getVarNames()));
        return vars;
    }

    private boolean containsVar(String var) { return getVarSet().contains(var);}

    public boolean containsAtom(Atomic atom){ return atomSet.contains(atom);}
    private boolean containsEquivalentAtom(Atomic atom){
        boolean isContained = false;

        Iterator<Atomic> it = atomSet.iterator();
        while( it.hasNext() && !isContained) {
            Atomic at = it.next();
            isContained = atom.isEquivalent(at);
        }

        return isContained;
    }

    private void replacePattern(PatternAdmin oldPattern, PatternAdmin newPattern) {
        PatternAdmin toRemove = oldPattern;
        for(PatternAdmin pat : pattern.getPatterns())
            if(pat.equals(oldPattern))
                toRemove = pat;

        pattern.getPatterns().remove(toRemove);
        pattern.getPatterns().add(newPattern);
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
        toRemove.forEach(atom -> toAdd.add(AtomicFactory.create(atom)));
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
        toRemove.forEach(atom -> toAdd.add(AtomicFactory.create(atom)));
        toRemove.forEach(this::removeAtom);
        toAdd.forEach(atom -> atom.unify(mappings));
        toAdd.forEach(this::addAtom);

        updateSelectedVars(mappings);
        resolveCaptures();
    }

    /**
     * finds captured variable occurrences in a query and replaces them with fresh variables
     */
    private void resolveCaptures() {
        //find captures
        Set<String> captures = new HashSet<>();
        getVarSet().forEach(v -> {
            if (v.contains("capture")) captures.add(v);
        });

        captures.forEach(cap -> {
            String fresh = createFreshVariable(getVarSet(), cap.replace("captured->", ""));
            unify(cap, fresh);
        });
    }

    public MatchQuery getMatchQuery() {
        if (selectVars.isEmpty())
            return Graql.match(pattern.getPatterns()).withGraph(graph);
        else
            return Graql.match(pattern.getPatterns()).select(selectVars).withGraph(graph);
    }

    private Map<Type, Set<Atomic>> getTypeAtomMap(Set<Atomic> atoms) {
        Map<Type, Set<Atomic>> map = new HashMap<>();
        for (Atomic atom : atoms) {
            Type type = graph.getType(atom.getTypeId());
            if (map.containsKey(type))
                map.get(type).add(atom);
            else
                map.put(type, Sets.newHashSet(atom));
        }
        return map;
    }

    public Map<String, Type> getVarTypeMap() {
        Map<String, Type> map = new HashMap<>();

        atomSet.forEach(atom -> {
            if (atom.isType() && !atom.isResource() ) {
                if (!atom.isRelation()) {
                    String var = atom.getVarName();
                    Type type = graph.getType(atom.getTypeId());
                    if (!map.containsKey(var))
                        map.put(var, type);
                    else
                        map.replace(var, type);
                }
                else {
                    Set<String> vars = atom.getVarNames();
                    vars.forEach(var -> {
                        if (!map.containsKey(var))
                            map.put(var, null);
                    });
                }
            }
        });

        return map;
    }

    public String getValue(String var) {
        String val ="";
        for(Atomic atom : atomSet) {
            if(atom.getVarName().equals(var))
                if(!atom.getVal().isEmpty() ) val = atom.getVal();
        }
        return val;
    }

    protected void addAtom(Atomic atom) {
        if(!containsAtom(atom)) {
            atomSet.add(atom);
            pattern.getPatterns().add(atom.getPattern());
        }
    }

    protected void removeAtom(Atomic atom) {
        atomSet.remove(atom);
        pattern.getPatterns().remove(atom.getPattern());
    }

    public void addAtomConstraints(Set<Atomic> subs){
        subs.forEach(con -> {
            if (containsVar(con.getVarName())){
                Atomic lcon = AtomicFactory.create(con);
                lcon.setParentQuery(this);
                addAtom(lcon);
                if (lcon.isSubstitution())
                    selectVars.remove(lcon.getVarName());
            }
        });
    }

    /**
     * atom selection function
     * @return selected atoms
     */
    public Set<Atomic> selectAtoms() {
        Set<Atomic> atoms = new HashSet<>(atomSet).stream()
                .filter(atom -> !atom.isSubstitution()).collect(Collectors.toSet());
        if (atoms.size() == 1) return atoms;

        Set<Atomic> selectedAtoms = atoms.stream()
                .filter(atom -> (!atom.isUnary()) || atom.isRuleResolvable() || atom.isResource())
                .collect(Collectors.toSet());
        if (selectedAtoms.isEmpty())
            throw new IllegalStateException(ErrorMessage.NO_ATOMS_SELECTED.getMessage(this.toString()));
        return selectedAtoms;

    }

    /**
     * @param q query to be compared with
     * @return true if two queries are alpha-equivalent
     */
    public boolean isEquivalent(Query q) {
        boolean equivalent = true;
        if(atomSet.size() != q.getAtoms().size()) return false;

        Iterator<Atomic> it = atomSet.iterator();
        while (it.hasNext() && equivalent) {
            Atomic atom = it.next();
            equivalent = q.containsEquivalentAtom(atom);
        }

        return equivalent;
    }

}
