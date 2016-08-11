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

package io.mindmaps.reasoner.internal.container;

import com.google.common.collect.Sets;
import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.core.model.Rule;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.api.parser.QueryParser;
import io.mindmaps.graql.api.query.Pattern;
import io.mindmaps.graql.api.query.MatchQuery;
import io.mindmaps.graql.api.query.QueryBuilder;
import io.mindmaps.graql.api.query.Var;
import io.mindmaps.reasoner.internal.predicate.Atomic;
import io.mindmaps.reasoner.internal.predicate.AtomicFactory;

import java.util.*;

import static io.mindmaps.reasoner.internal.Utility.isAtomRecursive;

public class Query {

    private final MindmapsTransaction graph;

    private final Set<Atomic> atomSet;
    private final Map<Type, Set<Atomic>> typeAtomMap;

    private MatchQuery matchQuery;

    private Atomic parentAtom = null;
    private Rule rule = null;

    public Query(String query, MindmapsTransaction transaction) {
        this.graph = transaction;
        QueryParser qp = QueryParser.create(graph);
        this.matchQuery = qp.parseMatchQuery(query).getMatchQuery();
        this.atomSet = getAtomSet(matchQuery);
        this.typeAtomMap = getTypeAtomMap(atomSet);
    }

    public Query(String query, Rule r, MindmapsTransaction transaction) {
        this.graph = transaction;
        QueryParser qp = QueryParser.create(graph);
        this.matchQuery = qp.parseMatchQuery(query).getMatchQuery();
        this.atomSet = getAtomSet(matchQuery);
        this.typeAtomMap = getTypeAtomMap(atomSet);
        this.rule = r;
    }

    public Query(MatchQuery query, MindmapsTransaction transaction) {
        this.graph = transaction;
        this.matchQuery = query;
        this.atomSet = getAtomSet(matchQuery);
        this.typeAtomMap = getTypeAtomMap(atomSet);
    }

    public Query(Query q) {
        this.graph = q.graph;
        QueryParser qp = QueryParser.create(graph);
        this.matchQuery = qp.parseMatchQuery(q.toString()).getMatchQuery();
        this.atomSet = getAtomSet(matchQuery);

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

    @Override
    public String toString() {
        return matchQuery.toString();
    }

    public void printAtoms() {
        atomSet.forEach(Atomic::print);
    }

    public void printTypeAtomMap() {
        for (Map.Entry<Type, Set<Atomic>> entry : typeAtomMap.entrySet()) {
            System.out.println("type: " + entry.getKey());
            entry.getValue().forEach(a -> System.out.println("atom: " + a.toString()));
        }
    }

    public MindmapsTransaction getTransaction(){ return graph;}
    public Rule getRule(){ return rule;}
    public Atomic getParentAtom(){ return parentAtom;}
    public void setParentAtom(Atomic par){ parentAtom = par;}
    public Query getParentQuery(){
        return parentAtom != null? parentAtom.getParentQuery() : null;
    }
    public Query getTopQueryWithRule(Rule rl)
    {
        Query topQuery = null;
        if (rule != null && rule.equals(rl)) topQuery = this;

        Query query = getParentQuery();

        while(query != null)
        {
            Rule currentRule = query.getRule();
            if (currentRule != null && currentRule.equals(rl)) topQuery = query;
            query = query.getParentQuery();
        }
        return topQuery;
    }

    public Atomic getTopAtom() {

        Atomic top = getParentAtom();
        Query parentQuery = top != null? top.getParentQuery() : null;
        while (parentQuery != null && parentQuery.getParentQuery() != null)
        {
            parentQuery = parentQuery.getParentQuery();
            if(parentQuery.getParentAtom() != null) top = parentQuery.getParentAtom();
        }
        return top;
    }

    public Query getTopQuery()
    {
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
            Pattern.Admin atomPattern = a.getPattern();
            Pattern.Admin expandedAtomPattern = a.getExpandedPattern();
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

    public boolean hasRecursiveAtoms()
    {
        boolean hasRecursiveAtoms = false;
        Iterator<Atomic> it = atomSet.iterator();
        while(it.hasNext() && !hasRecursiveAtoms)
            hasRecursiveAtoms = isAtomRecursive(it.next(), graph);

        return hasRecursiveAtoms;
    }

    //TODO Does it violate Horn clause limits?
    private void addPattern(Pattern.Admin newPattern) {
        matchQuery.admin().getPattern().getPatterns().add(newPattern);
    }

    private void replacePattern(Pattern.Admin oldPattern, Pattern.Admin newPattern) {
        Pattern.Admin toRemove = oldPattern;

        for(Pattern.Admin pat : matchQuery.admin().getPattern().getPatterns())
            if(pat.equals(oldPattern))
                toRemove = pat;

        matchQuery.admin().getPattern().getPatterns().remove(toRemove);
        matchQuery.admin().getPattern().getPatterns().add(newPattern);

    }

    private void updateSelectedVars(String from, String to)
    {
        Set<String> selectedVars = new HashSet<>(matchQuery.admin().getSelectedNames());
        if (selectedVars.contains(from))
        {
            selectedVars.remove(from);
            selectedVars.add(to);
            matchQuery = matchQuery.select(selectedVars);
        }
    }

    private void exchangeRelVarNames(String from, String to){
        changeVarName(to, "temp");
        changeVarName(from, to);
        changeVarName("temp", from);
    }

    public void changeRelVarNames(Map<String, String> mappings)
    {
        Map<String, String> appliedMappings = new HashMap<>();
        //do bidirectional mappings if any
        for (Map.Entry<String, String> mapping: mappings.entrySet())
        {
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

    /**NB: doesn't propagate to children if there are any*/
    public void changeVarName(String from, String to) {
        atomSet.forEach(atom -> atom.changeEachVarName(from, to));
        updateSelectedVars(from, to);
    }

    private Pattern.Disjunction<Pattern.Conjunction<Var.Admin>> getDNF(){
        return matchQuery.admin().getPattern().getDisjunctiveNormalForm();}

    private Set<AtomConjunction> getAtomConjunctions() {
        Set<AtomConjunction> conj = new HashSet<>();
        getDNF().getPatterns().forEach(c -> conj.add(new AtomConjunction(c)));
        return conj;
    }
    private Set<AtomConjunction> getExpandedAtomConjunctions() {
        Set<AtomConjunction> conj = new HashSet<>();
        getExpandedDNF().getPatterns().forEach(c -> conj.add(new AtomConjunction(c)));
        return conj;
    }
    private Pattern.Disjunction<Pattern.Conjunction<Var.Admin>> getExpandedDNF() {
        return getExpandedMatchQuery().admin().getPattern().getDisjunctiveNormalForm();
    }

    public MatchQuery getMatchQuery() {
        return matchQuery;
    }

    public MatchQuery getExpandedMatchQuery() {

        Set<String> selectVars = matchQuery.admin().getSelectedNames();
        Set<AtomConjunction> conjunctions = getAtomConjunctions();

        atomSet.forEach(atom -> {
            if (!atom.getExpansions().isEmpty())
            {
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
        QueryBuilder qb = QueryBuilder.build(graph);

        Set<Pattern.Conjunction<Var.Admin>> conjs = new HashSet<>();
        conjunctions.forEach(conj -> conjs.add(conj.getConjunction()));
        return qb.match(Pattern.Admin.disjunction(conjs)).select(selectVars);

    }

    public Pattern.Admin getPattern() {
        return getMatchQuery().admin().getPattern();
    }
    public Pattern.Admin getExpandedPattern() {
        return getExpandedMatchQuery().admin().getPattern();
    }

    private Set<Atomic> getAtomSet(MatchQuery query) {
        Set<Atomic> atoms = new HashSet<>();

        Set<Var.Admin> vars = query.admin().getPattern().getVars();
        vars.forEach(var ->
        {
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

        atomSet.forEach(atom ->
        {
            if (atom.isType() && !atom.isResource() ) {
                if (!atom.isRelation())
                {
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

    public String getValue(String var)
    {
        String val ="";
        for(Atomic atom : atomSet)
        {
            if(atom.getVarName().equals(var))
                if(!atom.getVal().isEmpty() ) val = atom.getVal();
        }
        return val;
    }

}
