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

package io.mindmaps.graql.internal.reasoner.container;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsTransaction;
import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.model.RelationType;
import io.mindmaps.core.model.RoleType;
import io.mindmaps.core.model.Rule;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.*;
import io.mindmaps.graql.admin.PatternAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.query.Conjunction;
import io.mindmaps.graql.internal.query.ConjunctionImpl;
import io.mindmaps.graql.internal.query.Disjunction;
import io.mindmaps.graql.internal.query.DisjunctionImpl;
import io.mindmaps.graql.internal.reasoner.predicate.Atomic;
import io.mindmaps.graql.internal.reasoner.predicate.AtomicFactory;
import io.mindmaps.graql.internal.reasoner.predicate.Relation;
import io.mindmaps.graql.internal.reasoner.predicate.Substitution;

import java.util.*;
import java.util.stream.Collectors;

import static io.mindmaps.graql.internal.reasoner.Utility.computeRoleCombinations;

public class  Query {

    private final MindmapsTransaction graph;

    private final Set<Atomic> atomSet;
    private final Map<Type, Set<Atomic>> typeAtomMap;

    private Atomic parentAtom = null;
    private Rule rule = null;

    private final Set<String> selectVars;
    private final Conjunction<PatternAdmin> pattern;

    public Query(String query, MindmapsTransaction transaction) {
        this.graph = transaction;
        QueryParser qp = QueryParser.create(graph);
        MatchQueryDefault matchQuery = qp.parseMatchQuery(query).getMatchQuery();
        this.pattern = matchQuery.admin().getPattern();
        this.selectVars = Sets.newHashSet(matchQuery.admin().getSelectedNames());

        this.atomSet = getAtomSet(pattern);
        this.typeAtomMap = getTypeAtomMap(atomSet);
    }

    public Query(String query, Rule r, MindmapsTransaction transaction) {
        this(query, transaction);
        this.rule = r;
    }

    public Query(MatchQueryDefault query, MindmapsTransaction transaction) {
        this.graph = transaction;

        this.pattern = query.admin().getPattern();
        this.selectVars = Sets.newHashSet(query.admin().getSelectedNames());

        this.atomSet = getAtomSet(pattern);
        this.typeAtomMap = getTypeAtomMap(atomSet);
    }

    public Query(Query q) {
        this.graph = q.graph;
        QueryParser qp = QueryParser.create(graph);

        MatchQueryDefault matchQuery = qp.parseMatchQuery(q.toString()).getMatchQuery();
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

    public Query(Atomic atom) {
        if (atom.getParentQuery() == null)
            throw new IllegalArgumentException(ErrorMessage.PARENT_MISSING.getMessage(atom.toString()));
        this.graph = atom.getParentQuery().getTransaction();
        this.pattern = new ConjunctionImpl<>(Sets.newHashSet());
        this.selectVars = Sets.newHashSet(atom.getMatchQuery(graph).admin().getSelectedNames());

        atomSet = new HashSet<>();
        addAtom(atom);
        addAtomConstraints(atom.getSubstitutions());
        if(atom.isRelation()) addAtomConstraints(atom.getTypeConstraints());

        this.typeAtomMap = getTypeAtomMap(atomSet);
    }

    @Override
    public String toString() { return getMatchQuery().toString();}

    public MindmapsTransaction getTransaction(){ return graph;}
    public Rule getRule(){ return rule;}
    public Atomic getParentAtom(){ return parentAtom;}
    public void setParentAtom(Atomic par){ parentAtom = par;}
    public Query getParentQuery(){
        return parentAtom != null? parentAtom.getParentQuery() : null;
    }

    /**
     * @param rl rule instance
     * @return top query in the tree corresponding to body of rule rl
     */
    public Query getTopQueryWithRule(Rule rl) {
        Query topQuery = null;
        if (rule != null && rule.equals(rl)) topQuery = this;

        Query query = getParentQuery();

        while(query != null){
            Rule currentRule = query.getRule();
            if (currentRule != null && currentRule.equals(rl)) topQuery = query;
            query = query.getParentQuery();
        }
        return topQuery;
    }

    /**
     * @return top atom of this branch
     */
    public Atomic getTopAtom() {
        Atomic top = getParentAtom();
        Query parentQuery = top != null? top.getParentQuery() : null;
        while (parentQuery != null && parentQuery.getParentQuery() != null) {
            parentQuery = parentQuery.getParentQuery();
            if(parentQuery.getParentAtom() != null) top = parentQuery.getParentAtom();
        }
        return top;
    }

    /**
     * @return top query in the tree
     */
    public Query getTopQuery() {
        if (getParentQuery() == null) return this;
        Query query = getParentQuery();
        while(query.getParentQuery() != null)
            query = query.getParentQuery();

        return query;
    }

    public Set<Atomic> getAtoms() { return atomSet;}
    public Set<Atomic> getAtomsWithType(Type type) {
        return typeAtomMap.get(type);
    }

    public Set<String> getSelectVars(){ return selectVars;}
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

    public boolean containsVar(String var) {
        boolean varContained = false;
        Iterator<Atomic> it = atomSet.iterator();
        while(it.hasNext() && !varContained)
            varContained = it.next().containsVar(var);

        return varContained;
    }

    public boolean containsAtom(Atomic atom){ return atomSet.contains(atom);}
    public boolean containsEquivalentAtom(Atomic atom){
        boolean isContained = false;

        Iterator<Atomic> it = atomSet.iterator();
        while( it.hasNext() && !isContained)
            isContained = atom.isEquivalent(it.next());

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

    private void updateSelectedVars(String from, String to) {
        if (selectVars.contains(from)) {
            selectVars.remove(from);
            selectVars.add(to);
        }
    }

    private void exchangeRelVarNames(String from, String to){
        changeVarName(to, "temp");
        changeVarName(from, to);
        changeVarName("temp", from);
    }

    public void changeRelVarNames(Map<String, String> mappings) {
        Map<String, String> appliedMappings = new HashMap<>();
        //do bidirectional mappings if any
        for (Map.Entry<String, String> mapping: mappings.entrySet()) {
            String varToReplace = mapping.getKey();
            String replacementVar = mapping.getValue();

            if(!appliedMappings.containsKey(varToReplace) || !appliedMappings.get(varToReplace).equals(replacementVar)) {
                /**bidirectional mapping*/
                if (mappings.containsKey(replacementVar) && mappings.get(replacementVar).equals(varToReplace)) {
                    exchangeRelVarNames(varToReplace, replacementVar);
                    appliedMappings.put(varToReplace, replacementVar);
                    appliedMappings.put(replacementVar, varToReplace);
                }
            }
        }
        mappings.entrySet().removeIf(e ->
                appliedMappings.containsKey(e.getKey()) && appliedMappings.get(e.getKey()).equals(e.getValue()));

        atomSet.forEach(atom -> atom.changeEachVarName(mappings));

        for (Map.Entry<String, String> mapping : mappings.entrySet())
            updateSelectedVars(mapping.getKey(), mapping.getValue());
    }

    public void changeVarName(String from, String to) {
        atomSet.forEach(atom -> atom.changeEachVarName(from, to));
        updateSelectedVars(from, to);
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

    public MatchQueryDefault getMatchQuery() { return Graql.match(pattern).select(selectVars).withTransaction(graph);}

    public MatchQueryDefault getExpandedMatchQuery() {
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
        QueryBuilder qb = Graql.withTransaction(graph);

        Set<Conjunction<VarAdmin>> conjs = new HashSet<>();
        conjunctions.forEach(conj -> conjs.add(conj.getConjunction()));
        return qb.match(new DisjunctionImpl<>(conjs)).select(selectVars);
    }

    public Conjunction<PatternAdmin> getPattern() {
        return pattern;
    }
    public PatternAdmin getExpandedPattern() {
        return getExpandedMatchQuery().admin().getPattern();
    }

    private Set<Atomic> getAtomSet(Conjunction<PatternAdmin> pat) {
        Set<Atomic> atoms = new HashSet<>();

        Set<VarAdmin> vars = pat.getVars();
        vars.forEach(var -> {
            Atomic atom = AtomicFactory.create(var, this);
            atoms.add(atom);
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

    private void addAtom(Atomic atom) {
        if(!containsAtom(atom)) {
            atomSet.add(atom);
            pattern.getPatterns().add(atom.getPattern());
        }
    }

    private void removeAtom(Atomic atom) {
        atomSet.remove(atom);
        pattern.getPatterns().remove(atom.getPattern());
    }

    public void addAtomConstraints(Set<Atomic> subs){
        subs.forEach(con -> {
            if (containsVar(con.getVarName())){
                //Atomic lcon = AtomicFactory.create(con);
                addAtom(con);
                if (con.isValuePredicate())
                    selectVars.remove(con.getVarName());
            }
        });
    }

    /**
     * atom selection function
     * @return selected atoms
     */
    public Set<Atomic> selectAtoms() {
        Set<Atomic> selectedAtoms = new LinkedHashSet<>();
        //TODO allow unary predicates
        Set<Atomic> atoms = new HashSet<>(atomSet).stream()
                .filter(atom -> !atom.isValuePredicate())
                .filter(atom -> !atom.isUnary())
                .collect(Collectors.toSet());
        if (atoms.size() == 1) return atoms;

        //find starting atom
        Atomic start = null;
        Iterator<Atomic> it = atoms.iterator();
        while(it.hasNext() && start == null) {
            Atomic current = it.next();
            if (current.getNeighbours().size() == 1) start = current;
        }
        if (start == null )
            throw new IllegalStateException(ErrorMessage.LOOP_CLAUSE.getMessage());

        selectedAtoms.add(start);

        Atomic current = start;
        Set<Atomic> nbhs;
        do{
            nbhs = current.getNeighbours();
            nbhs.removeAll(selectedAtoms);
            if (nbhs.size() == 1 ){
                current = nbhs.iterator().next();
                selectedAtoms.add(current);
            }
        } while(!nbhs.isEmpty());

        //TODO
        //find most instantiated atom
        //no instantiations -> favour type atoms
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
        while (it.hasNext() && equivalent)
            equivalent = q.containsEquivalentAtom(it.next());

        return equivalent;
    }

    private void materialize() {
        if (!getMatchQuery().ask().execute()) {
            InsertQuery insert = Graql.insert(getPattern().getVars()).withTransaction(graph);
            insert.execute();
        }
    }

    //only for atomic queries
    public void materialize(Set<Substitution> subs) {
        subs.forEach(this::addAtom);

        //extrapolate if needed
        Atomic atom = selectAtoms().iterator().next();
        if(atom.isRelation() && ((Relation) atom).getRoleVarTypeMap().isEmpty()){
            String relTypeId = atom.getTypeId();
            RelationType relType = graph.getRelationType(relTypeId);
            Set<String> vars = atom.getVarNames();
            Set<RoleType> roles = Sets.newHashSet(relType.hasRoles());

            Set<Map<String, String>> roleMaps = new HashSet<>();
            computeRoleCombinations(vars, roles, new HashMap<>(), roleMaps);

            removeAtom(atom);
            roleMaps.forEach( map -> {
                Relation relationWithRoles = new Relation(relTypeId, map);
                addAtom(relationWithRoles);
                materialize();
                removeAtom(relationWithRoles);
            });
            addAtom(atom);
        }
        else
            materialize();

        subs.forEach(this::removeAtom);
    }

}
