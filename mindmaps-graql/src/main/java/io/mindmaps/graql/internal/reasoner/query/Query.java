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
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.admin.Conjunction;
import io.mindmaps.graql.admin.Disjunction;
import io.mindmaps.graql.internal.query.match.MatchOrder;
import io.mindmaps.graql.internal.query.match.MatchQueryInternal;
import io.mindmaps.graql.internal.reasoner.container.AtomConjunction;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.graql.admin.PatternAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.query.Patterns;
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
    private Atomic parentAtom = null;

    public Query(String query, MindmapsGraph graph) {
        this.graph = graph;
        MatchQuery matchQuery = Graql.withGraph(graph).parseMatch(query);
        this.pattern = matchQuery.admin().getPattern();
        this.selectVars = Sets.newHashSet(matchQuery.admin().getSelectedNames());

        this.atomSet = getAtomSet(pattern);
        this.typeAtomMap = getTypeAtomMap(atomSet);
    }

    public Query(MatchQuery query, MindmapsGraph graph) {
        this.graph = graph;

        this.pattern = query.admin().getPattern();
        this.selectVars = Sets.newHashSet(query.admin().getSelectedNames());

        this.atomSet = getAtomSet(pattern);
        this.typeAtomMap = getTypeAtomMap(atomSet);
    }

    public Query(Query q) {
        this.graph = q.graph;

        MatchQuery matchQuery = q.getMatchQuery();
        this.pattern = matchQuery.admin().getPattern();
        this.selectVars = Sets.newHashSet(matchQuery.admin().getSelectedNames());
        this.atomSet = getAtomSet(pattern);

        //copy expansions
        for (Atomic qAtom : q.atomSet) {
            Set<Query> expansions = qAtom.getExpansions();
            for (Query exp : expansions) {
                atomSet.forEach(atom ->
                {
                    if (atom.equals(qAtom)) atom.addExpansion(new Query(exp));
                });
            }
        }
        this.typeAtomMap = getTypeAtomMap(atomSet);
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

    public void setParentAtom(Atomic par){ parentAtom = par;}
    public Set<Atomic> getAtoms() { return new HashSet<>(atomSet);}
    public Set<Atomic> getAtomsWithType(Type type) {
        return typeAtomMap.get(type);
    }
    public Set<Atomic> getSubstitutions(){
        return getAtoms().stream().filter(Atomic::isValuePredicate).collect(Collectors.toSet());
    }

    public Set<String> getVarSet() {
        Set<String> vars = new HashSet<>();
        atomSet.forEach(atom -> vars.addAll(atom.getVarNames()));
        return vars;
    }

    public void expandAtomByQuery(Atomic atom, Query query) {
        atomSet.stream().filter(a -> a.equals(atom)).forEach(a -> a.addExpansion(query));
    }

    public void removeExpansionFromAtom(Atomic atom, Query query) {
        atomSet.stream().filter(a -> a.equals(atom)).forEach(a -> {
            PatternAdmin atomPattern = a.getPattern();
            PatternAdmin expandedAtomPattern = a.getExpandedPattern();
            a.removeExpansion(query);

            replacePattern(expandedAtomPattern, atomPattern);
        });
    }

    private boolean containsVar(String var) {
        boolean varContained = false;
        Iterator<Atomic> it = atomSet.iterator();
        while(it.hasNext() && !varContained)
            varContained = it.next().containsVar(var);

        return varContained;
    }

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
        changeVarName(to, "temp");
        changeVarName(from, to);
        changeVarName("temp", from);
    }

    public void changeVarName(String from, String to) {
        Set<Atomic> toRemove = new HashSet<>();
        Set<Atomic> toAdd = new HashSet<>();

        atomSet.stream().filter(atom -> atom.getVarNames().contains(from)).forEach(toRemove::add);
        toRemove.forEach(atom -> toAdd.add(AtomicFactory.create(atom)));
        toRemove.forEach(this::removeAtom);
        toAdd.forEach(atom -> atom.changeEachVarName(from, to));
        toAdd.forEach(this::addAtom);

        Map<String, String> mapping = new HashMap<>();
        mapping.put(from, to);
        updateSelectedVars(mapping);
    }

    public void unify(Map<String, String> unifiers, Set<String> globalVars) {
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
                    Set<String> intersection = atom.getVarNames();
                    atom.getVarNames().retainAll(mappings.keySet());
                    return !intersection.isEmpty();
                })
                .forEach(toRemove::add);
        toRemove.forEach(atom -> toAdd.add(AtomicFactory.create(atom)));
        toRemove.forEach(this::removeAtom);
        toAdd.forEach(atom -> atom.changeEachVarName(mappings));
        toAdd.forEach(this::addAtom);

        updateSelectedVars(mappings);
        resolveCaptures(globalVars);
    }

    /**
     * finds captured variable occurrences in a query and replaces them with fresh variables
     * @param globalVars global variables to be avoided when creating fresh variables
     */
    private void resolveCaptures(Set<String> globalVars) {
        //find captures
        Set<String> captures = new HashSet<>();
        getVarSet().forEach(v -> {
            if (v.contains("capture")) captures.add(v);
        });

        captures.forEach(cap -> {
            String fresh = createFreshVariable(globalVars, getVarSet(), cap.replace("captured->", ""));
            changeVarName(cap, fresh);
        });
    }

    private Disjunction<Conjunction<VarAdmin>> getDNF(){
        return pattern.getDisjunctiveNormalForm();}

    /**
     * @return set of conjunctions from the DNF
     */
    private Set<AtomConjunction> getAtomConjunctions() {
        Set<AtomConjunction> conj = new HashSet<>();
        getDNF().getPatterns().forEach(c -> conj.add(new AtomConjunction(c)));
        return conj;
    }

    /**
     * @return set of conjunctions from the DNF taking into account atom expansions
     */
    private Set<AtomConjunction> getExpandedAtomConjunctions() {
        Set<AtomConjunction> conj = new HashSet<>();
        getExpandedDNF().getPatterns().forEach(c -> conj.add(new AtomConjunction(c)));
        return conj;
    }

    private Disjunction<Conjunction<VarAdmin>> getExpandedDNF() {
        return getExpandedMatchQuery().admin().getPattern().getDisjunctiveNormalForm();
    }

    public MatchQuery getMatchQuery() {
        if (selectVars.isEmpty())
            return Graql.match(pattern).withGraph(graph);
        else
            return Graql.match(pattern).select(selectVars).withGraph(graph);
    }

    public MatchQuery getExpandedMatchQuery() {
        Set<AtomConjunction> conjunctions = getAtomConjunctions();
        atomSet.forEach(atom -> {
            if (!atom.getExpansions().isEmpty()) {
                //find base conjunctions
                Set<AtomConjunction> baseConjunctions = new HashSet<>();
                conjunctions.forEach(conj -> {
                    if(conj.contains(atom))
                        baseConjunctions.add(conj.remove(atom));
                });

                for (Query exp : atom.getExpansions()) {
                    Set<AtomConjunction> childConjunctions = exp.getExpandedAtomConjunctions();

                    childConjunctions.forEach(chConj -> {
                        baseConjunctions.forEach( bConj -> {
                            AtomConjunction conj = bConj.conjunction(chConj, graph);
                            if (conj != null) conjunctions.add(conj);
                        });
                    });
                }
            }
        });
        QueryBuilder qb = Graql.withGraph(graph);

        Set<Conjunction<VarAdmin>> conjs = new HashSet<>();
        conjunctions.forEach(conj -> conjs.add(conj.getConjunction()));
        return qb.match(Patterns.disjunction(conjs)).select(selectVars);
    }

    public PatternAdmin getExpandedPattern() {
        return getExpandedMatchQuery().admin().getPattern();
    }

    private Set<Atomic> getAtomSet(Conjunction<PatternAdmin> pat) {
        Set<Atomic> atoms = new HashSet<>();

        Set<VarAdmin> vars = pat.getVars();
        vars.forEach(var -> {
            if(var.getType().isPresent() && (var.getId().isPresent() || !var.getValueEqualsPredicates().isEmpty())) {
                VarAdmin typeVar = Graql.var(var.getName()).isa(var.getType().orElse(null)).admin();
                atoms.add(AtomicFactory.create(typeVar, this));

                if (var.getId().isPresent()) {
                    VarAdmin sub = Graql.var(var.getName()).id(var.getId().orElse(null)).admin();
                    atoms.add(AtomicFactory.create(sub, this));
                }
                else if (!var.getValueEqualsPredicates().isEmpty()){
                    if(var.getValueEqualsPredicates().size() > 1)
                        throw new IllegalArgumentException(ErrorMessage.MULTI_VALUE_VAR.getMessage(var.toString()));
                    VarAdmin sub = Graql.var(var.getName()).value(var.getValueEqualsPredicates().iterator().next()).admin();
                    atoms.add(AtomicFactory.create(sub, this));
                }
            }
            else
                atoms.add(AtomicFactory.create(var, this));
        });

        return atoms;
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
                if (lcon.isValuePredicate())
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
                .filter(atom -> !atom.isValuePredicate()).collect(Collectors.toSet());
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
