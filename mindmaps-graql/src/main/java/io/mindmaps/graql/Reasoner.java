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

package io.mindmaps.graql;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.RoleType;
import io.mindmaps.core.model.Rule;
import io.mindmaps.core.model.Type;
import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.graql.internal.reasoner.container.Query;
import io.mindmaps.graql.internal.reasoner.predicate.Atomic;
import io.mindmaps.graql.internal.reasoner.predicate.Relation;
import io.mindmaps.graql.internal.reasoner.predicate.Substitution;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static io.mindmaps.graql.internal.reasoner.Utility.*;


public class Reasoner {

    private final MindmapsGraph graph;
    private final QueryParser qp;
    private final Logger LOG = LoggerFactory.getLogger(Reasoner.class);

    private final Map<String, Query> workingMemory = new HashMap<>();

    public Reasoner(MindmapsGraph graph){
        this.graph = graph;
        qp =  QueryParser.create(graph);

        linkConceptTypes();
    }

    private boolean checkChildApplicableToAtomViaRelation(Relation parentAtom, Query parent, Query childLHS, Query childRHS) {
        boolean relRelevant = true;
        Atomic childAtom = getRuleConclusionAtom(childLHS, childRHS);
        Map<RoleType, Pair<String, Type>> childRoleVarTypeMap = ((Relation) childAtom).getRoleVarTypeMap();

        /**Check for role compatibility*/
        Map<RoleType, Pair<String, Type>> parentRoleVarTypeMap = parentAtom.getRoleVarTypeMap();
        for (Map.Entry<RoleType, Pair<String, Type>> entry : parentRoleVarTypeMap.entrySet()) {
            RoleType role = entry.getKey();
            Type pType = entry.getValue().getValue();
            /**vars can be matched by role types*/
            if (childRoleVarTypeMap.containsKey(role)) {
                Type chType = childRoleVarTypeMap.get(role).getValue();
                /**check type compatibility*/
                if (chType != null) {
                    relRelevant &= pType.equals(chType) || chType.subTypes().contains(pType);

                    /**Check for any constraints on the variables*/
                    String chVar = childRoleVarTypeMap.get(role).getKey();
                    String pVar = entry.getValue().getKey();
                    String chVal = childLHS.getValue(chVar);
                    String pVal = parent.getValue(pVar);
                    if ( !chVal.isEmpty() && !pVal.isEmpty())
                        relRelevant &= chVal.equals(pVal);
                }
            }
        }

        return relRelevant;
    }

    private boolean checkChildApplicableViaRelation(Query parent, Query childLHS, Query childRHS, Type relType) {
        boolean relRelevant = false;
        Set<Atomic> relevantAtoms = parent.getAtomsWithType(relType);
        Iterator<Atomic> it = relevantAtoms.iterator();
        while(it.hasNext() && !relRelevant)
            relRelevant = checkChildApplicableToAtomViaRelation((Relation) it.next(), parent, childLHS, childRHS);

        return relRelevant;
    }

    private boolean checkChildApplicableToAtomViaResource(Atomic parent, Query childLHS, Query childRHS) {
        Atomic childAtom = getRuleConclusionAtom(childLHS, childRHS);
        String childVal = childAtom.getVal();
        String val = parent.getVal();

        return val.equals(childVal);
    }

    private boolean checkChildApplicableViaResource(Query parent, Query childLHS, Query childRHS, Type type) {
        boolean resourceApplicable = false;

        Set<Atomic> atoms = parent.getAtomsWithType(type);
        Iterator<Atomic> it = atoms.iterator();
        while(it.hasNext() && !resourceApplicable)
            resourceApplicable = checkChildApplicableToAtomViaResource(it.next(), childLHS, childRHS);

        return resourceApplicable;
    }

    private Set<Rule> getQueryChildren(Query query) {
        Set<Rule> children = new HashSet<>();
        query.getAtoms().stream().filter(Atomic::isType).forEach(atom -> children.addAll(getAtomChildren(atom)));
        return children;
    }

    private Set<Rule> getAtomChildren(Atomic atom) {
        Set<Rule> children = new HashSet<>();
        Query parent = atom.getParentQuery();

        String typeId = atom.getTypeId();
        if (typeId.isEmpty()) return children;
        Type type = graph.getType(typeId);

        Collection<Rule> rulesFromType = type.getRulesOfConclusion();

        for (Rule rule : rulesFromType) {
            boolean ruleRelevant = true;
            if (atom.isResource())
                        ruleRelevant = checkChildApplicableToAtomViaResource(atom,
                                workingMemory.get(rule.getId()), new Query(rule.getRHS(), graph));
            else if (atom.isRelation()) {
                LOG.debug("Checking relevance of rule " + rule.getId());
                ruleRelevant = checkChildApplicableToAtomViaRelation((Relation) atom, parent,
                        workingMemory.get(rule.getId()), new Query(rule.getRHS(), graph));
                if (!ruleRelevant)
                    LOG.debug("Rule " + rule.getId() + " not relevant through type " + type.getId());
            }

            if (ruleRelevant) children.add(rule);
        }

        return children;
    }


    private Set<Rule> getRuleChildren(Rule parent) {
        Set<Rule> children = new HashSet<>();
        Collection<Type> types = parent.getHypothesisTypes();
        for( Type type : types) {
            Collection<Rule> rulesFromType = type.getRulesOfConclusion();
            for (Rule rule : rulesFromType) {
                boolean ruleRelevant = true;
                if (type.isResourceType())
                    ruleRelevant = checkChildApplicableViaResource(workingMemory.get(parent.getId()),
                            workingMemory.get(rule.getId()), new Query(rule.getRHS(), graph), type);
                else if (type.isRelationType())
                    ruleRelevant = checkChildApplicableViaRelation(workingMemory.get(parent.getId()),
                            workingMemory.get(rule.getId()), new Query(rule.getRHS(), graph), type);

                if (!rule.equals(parent) && ruleRelevant ) children.add(rule);
                else{
                    LOG.debug("in getRuleChildren: Rule " + rule.getId() + " not relevant to type " + type.getId() + " " + ruleRelevant);
                }
            }
        }

        return children;
    }

    private void linkConceptTypes(Rule rule)
    {
        LOG.debug("Linking rule " + rule.getId() + "...");
        MatchQuery qLHS = qp.parseMatchQuery(rule.getLHS()).getMatchQuery();
        MatchQuery qRHS = qp.parseMatchQuery(rule.getRHS()).getMatchQuery();

        Set<Type> hypothesisConceptTypes = qLHS.admin().getTypes();
        Set<Type> conclusionConceptTypes = qRHS.admin().getTypes();

        hypothesisConceptTypes.forEach(rule::addHypothesis);
        conclusionConceptTypes.forEach(rule::addConclusion);

        LOG.debug("Rule " + rule.getId() + " linked");

    }

    public Set<Rule> getRules() {
        Set<Rule> rules = new HashSet<>();
        MatchQuery sq = qp.parseMatchQuery("match $x isa inference-rule;").getMatchQuery();

        List<Map<String, Concept>> results = Lists.newArrayList(sq);

        for (Map<String, Concept> result : results) {
            for (Map.Entry<String, Concept> entry : result.entrySet()) {
                Concept concept = entry.getValue();
                rules.add((Rule) concept);
            }
        }

        return rules;
    }

    /**
     * Link all unlinked rules in the rule base to their matching types
     */
    public void linkConceptTypes() {
        Set<Rule> rules = getRules();
        LOG.debug(rules.size() + " rules initialized...");

        for(Rule rule : rules) {
            workingMemory.putIfAbsent(rule.getId(), new Query(rule.getLHS(), graph));
            if(rule.getHypothesisTypes().isEmpty() && rule.getConclusionTypes().isEmpty()) {
                linkConceptTypes(rule);
            }
        }
    }

    private void restoreWM() {
        workingMemory.clear();
        Set<Rule> rules = getRules();

        for (Rule rule : rules) {
            workingMemory.putIfAbsent(rule.getId(), new Query(rule.getLHS(), graph));
        }
    }

    /**
     * generate a fresh variable avoiding global variables and variables from the same query
     * @param globalVars global variables to avoid
     * @param childVars variables from the query var belongs to
     * @param var variable to be generated a fresh replacement
     * @return fresh variables
     */
    private String createFreshVariable(Set<String> globalVars, Set<String> childVars, String var) {
        String fresh = var;
        while(globalVars.contains(fresh) || childVars.contains(fresh)) {
            String valFree = fresh.replaceAll("[^0-9]", "");
            int value = valFree.equals("") ? 0 : Integer.parseInt(valFree);
            fresh = fresh.replaceAll("\\d+", "") + (++value);
        }
        return fresh;
    }

    /**
     * finds captured variable occurrences in a query and replaces them with fresh variables
     * @param query input query with var captures
     * @param globalVars global variables to be avoided when creating fresh variables
     */
    private void resolveCaptures(Query query, Set<String> globalVars) {
        //find captures
        Set<String> captures = new HashSet<>();
        query.getVarSet().forEach(v -> {
            if (v.contains("capture")) captures.add(v);
        });

        captures.forEach(cap -> {
            String fresh = createFreshVariable(globalVars, query.getVarSet(), cap.replace("captured->", ""));
            query.changeVarName(cap, fresh);
        });
    }

    private Map<String, String> getUnifiers(Atomic childAtom, Atomic parentAtom) {
        Set<String> varsToAllocate = parentAtom.getVarNames();

        Set<String> childBVs = childAtom.getVarNames();

        Map<String, String> unifiers = new HashMap<>();
        Map<String, Pair<Type, RoleType>> childMap = childAtom.getVarTypeRoleMap();
        Map<RoleType, Pair<String, Type>> parentMap = parentAtom.getRoleVarTypeMap();

        for (String chVar : childBVs) {
            RoleType role = childMap.containsKey(chVar)? childMap.get(chVar).getValue(): null;
            String pVar = role != null && parentMap.containsKey(role) ? parentMap.get(role).getKey() : "";
            if (pVar.isEmpty())
                pVar = varsToAllocate.iterator().next();

            if ( !chVar.equals(pVar))
                unifiers.put(chVar, pVar);

            varsToAllocate.remove(pVar);
        }

        return unifiers;
    }

    /**
     * propagate variables to child via a relation atom (atom variables are bound)
     * @param childHead child rule head
     * @param childBody child rule body
     * @param parentAtom parent atom (predicate) being resolved (subgoal)
     * @param parentLHS parent query
     * @param globalVarMap map containing global vars and their types
     */
    private void unifyRuleViaAtom(Query childHead, Query childBody, Atomic parentAtom, Query parentLHS,
                                             Map<String, Type> globalVarMap) {
        Atomic childAtom = getRuleConclusionAtom(childBody, childHead);
        Map<String, String> unifiers = getUnifiers(childAtom, parentAtom);

        /**do alpha-conversion*/
        childHead.changeVarNames(unifiers);
        childBody.changeVarNames(unifiers);
        resolveCaptures(childBody, globalVarMap.keySet());

        /**check free variables for possible captures*/
        Set<String> childFVs = childBody.getVarSet();
        Set<String> parentBVs = parentAtom.getVarNames();
        Set<String> parentVars = parentLHS.getVarSet();
        parentBVs.forEach(childFVs::remove);

        childFVs.forEach(chVar -> {
            // if (x e P) v (x e G)
            // x -> fresh
                if (parentVars.contains(chVar) || globalVarMap.containsKey(chVar)) {
                    String freshVar = createFreshVariable(globalVarMap.keySet(), childBody.getVarSet(), chVar);
                    childBody.changeVarName(chVar, freshVar);
                }
            });
        }

    /**
     * make child query consistent by performing variable substitution so that parent variables are propagated
     * @param rule to be unified
     * @param parentAtom parent atom (predicate) being resolved (subgoal)
     * @param globalVarMap map containing global vars and their types
     */
    private Pair<Query, Query> unifyRule(Rule rule, Atomic parentAtom, Map<String, Type> globalVarMap) {
        Query parent = parentAtom.getParentQuery();
        Query ruleBody = new Query(rule.getLHS(), graph);
        Query ruleHead= new Query(rule.getRHS(), graph);

        unifyRuleViaAtom(ruleHead, ruleBody, parentAtom, parent, globalVarMap);

        //update global vars
        Map<String, Type> varTypeMap = ruleBody.getVarTypeMap();
        for(Map.Entry<String, Type> entry : varTypeMap.entrySet())
            globalVarMap.putIfAbsent(entry.getKey(), entry.getValue());

        return new Pair<>(ruleBody, ruleHead);
    }

    private Query applyRuleToAtom(Atomic parentAtom, Rule child, Map<String, Type> varMap) {
        Query ruleBody = unifyRule(child, parentAtom, varMap).getKey();
        parentAtom.addExpansion(ruleBody);

        return ruleBody;
    }

    private Set<Query> applyRuleToQuery(Query parent, Rule child, Map<String, Type> varMap) {
        Type type = getRuleConclusionType(child);
        Set<Query> expansions = new HashSet<>();

        Set<Atomic> atoms = parent.getAtomsWithType(type);
        if (atoms == null) return expansions;

        atoms.forEach(atom -> expansions.add(applyRuleToAtom(atom, child, varMap)));

        LOG.debug("EXPANDED: Parent\n" + parent.toString() + "\nEXPANDED by " + child.getId() + " through type " + type.getId());

        return expansions;
    }

    private Set<Map<String, Concept>> DBlookup(Query query) {
        Set<Map<String, Concept>> answers = new HashSet<>();
        Lists.newArrayList(query.getMatchQuery().distinct()).forEach(answers::add);
        return answers;
    }

    private Set<Map<String, Concept>> getUnifiedAnswers(Query parentQuery, Query childQuery,
                                                        Set<Map<String, Concept>>answers){
        Set<Atomic> childAtoms = childQuery.selectAtoms();
        Set<Atomic> parentAtoms = parentQuery.selectAtoms();
        if(childAtoms.size() > 1 || parentAtoms.size() > 1) {
            throw new IllegalArgumentException(ErrorMessage.NON_ATOMIC_QUERY.getMessage());
        }

        Atomic childAtom = childAtoms.iterator().next();
        Atomic parentAtom = parentAtoms.iterator().next();
        Map<String, String> unifiers = getUnifiers(childAtom, parentAtom);
        Set<Map<String, Concept>> unifiedAnswers = new HashSet<>();
        answers.forEach( entry -> {
            Map<String, Concept> answer = new HashMap<>();
            entry.forEach( (var, con) -> {
                if (unifiers.containsKey(var))
                    answer.put(unifiers.get(var), con);
                else
                    answer.put(var, con);
            });
            unifiedAnswers.add(answer);
        });

        return unifiedAnswers;
    }

    private Set<Map<String, Concept>> memoryLookup(Query query, Map<Query, Set<Map<String, Concept>>> matAnswers) {
        Set<Map<String, Concept>> answers = new HashSet<>();
        Query equivalentQuery = findEquivalentQuery(query, matAnswers.keySet());
        if (equivalentQuery != null)
            answers = getUnifiedAnswers(query, equivalentQuery, matAnswers.get(equivalentQuery));

        //TODO get answers from compatible queries
        return answers;
    }

    private Set<Map<String, Concept>> propagateHeadSubstitutions(Query atomicQuery, Query ruleHead, Set<Map<String, Concept>> answers){
        Set<Map<String, Concept>> newAnswers = new HashSet<>();
        if(answers.isEmpty()) return newAnswers;

        Set<String> queryVars = atomicQuery.getSelectVars();
        Set<String> headVars = ruleHead.getSelectVars();
        Set<Atomic> extraSubs = new HashSet<>();
        if(queryVars.size() > headVars.size()){
            extraSubs.addAll(ruleHead.getSubstitutions()
                    .stream().filter(sub -> queryVars.contains(sub.getVarName()))
                    .collect(Collectors.toSet()));
        }

        answers.forEach( map -> {
            Map<String, Concept> newMap = new HashMap<>(map);
            extraSubs.forEach(sub -> newMap.put(sub.getVarName(), graph.getInstance(sub.getVal())) );
            newAnswers.add(newMap);
        });

        return newAnswers;
    }

    private Set<Map<String, Concept>> filterAnswers(Query atomicQuery, Set<Map<String, Concept>> answers) {
        Set<Map<String, Concept>> results =  new HashSet<>();
        if(answers.isEmpty()) return results;

        Set<String> queryVars = atomicQuery.getSelectVars();
        answers.forEach(answer -> {
            Map<String, Concept> map = new HashMap<>();
            answer.forEach((var, concept) -> {
                if(queryVars.contains(var))
                    map.put(var, concept);
            });
            if (!map.isEmpty()) results.add(map);
        });

        return results.stream().distinct().collect(Collectors.toSet());
    }

    private void propagateAnswers(Query query, Set<Map<String, Concept>> answers, Map<Query, Set<Map<String, Concept>>> matAnswers){
        if(answers.isEmpty()) return;

        Set<Atomic> atoms = query.selectAtoms();
        if (atoms.size() != 1 )
            throw new IllegalArgumentException(ErrorMessage.NON_ATOMIC_QUERY.getMessage());
        Atomic atom = atoms.iterator().next();
        Set<Atomic> constraints = atom.getSubstitutions();
        constraints.addAll(atom.getTypeConstraints());

        //find compatible queries by stripping constraints
        Query genQuery = new Query(query);
        Iterator<Atomic> it = constraints.iterator();
        while(it.hasNext() && genQuery.getAtoms().size() != 1) {
            Atomic cstr = it.next();
            genQuery.getAtoms().remove(cstr);
            Query compatibleQuery = findEquivalentQuery(genQuery, matAnswers.keySet());
            if(compatibleQuery != null) {
                Set<Map<String, Concept>> compatibleAnswers = new HashSet<>();
                answers.forEach( entry -> compatibleAnswers.add(new HashMap<>(entry)));
                if (cstr.isValuePredicate())
                    compatibleAnswers.forEach(map -> map.put(cstr.getVarName(), graph.getInstance(cstr.getVal())));
                Set<Map<String, Concept>> unifiedAnswers = getUnifiedAnswers(compatibleQuery, query, compatibleAnswers);
                matAnswers.get(compatibleQuery).addAll(unifiedAnswers);
            }
        }
    }

    private void recordAnswers(Query query, Set<Map<String, Concept>> answers,
                               Map<Query, Set<Map<String, Concept>>> matAnswers) {
        Query equivalentQuery = findEquivalentQuery(query, matAnswers.keySet());
        if (equivalentQuery != null) {
            Set<Map<String, Concept>> unifiedAnswers = getUnifiedAnswers(equivalentQuery, query, answers);
            matAnswers.get(equivalentQuery).addAll(unifiedAnswers);
            propagateAnswers(query, unifiedAnswers, matAnswers);
        }
        else {
            matAnswers.put(query, answers);
            propagateAnswers(query, answers, matAnswers);
        }
    }

    private void materializeAnswers(Query atomicQuery, Set<Map<String, Concept>> answers){
        LOG.debug("Materializing...");
        answers.forEach(answer -> {
            Set<Substitution> subs = new HashSet<>();
            answer.forEach((var, con) -> {
                Substitution sub = new Substitution(var, con);
                if (!atomicQuery.containsAtom(sub))
                    subs.add(sub);
            });
            atomicQuery.materialize(subs);
        });
        LOG.debug("Materialized: " + answers.size());
    }

    private Set<Map<String, Concept>> joinSubstitutions(Set<Map<String, Concept>> tuples, Set<Map<String, Concept>> localTuples) {
        if(localTuples.isEmpty())
            return new HashSet<>();
        if(tuples.isEmpty())
            return Sets.newHashSet(localTuples);

        Set<Map<String, Concept>> join = new HashSet<>();
        for( Map<String, Concept> lanswer : localTuples){
            for (Map<String, Concept> answer : tuples){
                boolean isCompatible = true;
                Iterator<Map.Entry<String, Concept>> it = lanswer.entrySet().iterator();
                while(it.hasNext() && isCompatible) {
                    Map.Entry<String, Concept> entry = it.next();
                    String var = entry.getKey();
                    Concept concept = entry.getValue();
                    if(answer.containsKey(var) && !concept.equals(answer.get(var)))
                        isCompatible = false;
                }

                if (isCompatible) {
                    Map<String, Concept> merged = new HashMap<>();
                    merged.putAll(lanswer);
                    merged.putAll(answer);
                    join.add(merged);
                }
            }
        }
        return join;
    }

    private void propagateConstraints(Atomic parentAtom, Query ruleHead, Query ruleBody){
        ruleHead.addAtomConstraints(parentAtom.getSubstitutions());
        ruleHead.addAtomConstraints(ruleBody.getSubstitutions());
        ruleBody.addAtomConstraints(parentAtom.getSubstitutions());
        if(parentAtom.isRelation()) {
            ruleHead.addAtomConstraints(parentAtom.getTypeConstraints());
            ruleBody.addAtomConstraints(parentAtom.getTypeConstraints());
        }
    }

    private Set<Map<String, Concept>> Answer(Atomic atom, Set<Query> subGoals, Map<String, Type> varMap) {
        Query atomicQuery = new Query(atom);
        Set<Map<String, Concept>> allSubs = DBlookup(atomicQuery);

        boolean queryAdmissible = true;
        Iterator<Query> it = subGoals.iterator();
        while( it.hasNext() && queryAdmissible)
            queryAdmissible = !atomicQuery.isEquivalent(it.next());

        if(queryAdmissible) {
            Set<Rule> rules = getAtomChildren(atom);
            for (Rule rule : rules) {
                Pair<Query, Query> ruleQuery = unifyRule(rule, atom, varMap);
                Query ruleBody = ruleQuery.getKey();
                Query ruleHead = ruleQuery.getValue();

                propagateConstraints(atom, ruleHead, ruleBody);

                Set<Map<String, Concept>> subs = new HashSet<>();
                Set<Atomic> atoms = ruleBody.selectAtoms();

                Iterator<Atomic> atIt = atoms.iterator();
                do{
                    Atomic at = atIt.next();
                    subGoals.add(atomicQuery);
                    Set<Map<String, Concept>> localSubs = Answer(at, subGoals, varMap);
                    subs = joinSubstitutions(subs, localSubs);
                } while(!subs.isEmpty() && atIt.hasNext());

                Set<Map<String, Concept>> answers = propagateHeadSubstitutions(atomicQuery, ruleHead, subs);
                Set<Map<String, Concept>> filteredAnswers = filterAnswers(atomicQuery, answers);
                materializeAnswers(ruleHead, filteredAnswers);
                allSubs.addAll(filteredAnswers);
            }
        }

        return allSubs;
    }

    private Set<Map<String, Concept>> resolveAtomicQuery(Atomic atom) {
        Set<Map<String, Concept>> subAnswers = new HashSet<>();
        int dAns;
        int iter = 0;

        do {
            Set<Query> subGoals = new HashSet<>();
            Map<String, Type> varMap = atom.getParentQuery().getVarTypeMap();
            dAns = subAnswers.size();
            LOG.debug("iter: " + iter++ + " answers: " + dAns);
            subAnswers = Answer(atom, subGoals, varMap);
            dAns = subAnswers.size() - dAns;
        } while(dAns != 0);

        return subAnswers;
    }

    private Set<Map<String, Concept>> resolveQuery(Query query) {
        if(getRules().isEmpty())
            return Sets.newHashSet(query.getMatchQuery().distinct());

        Set<Map<String, Concept>> answers = new HashSet<>();
        Set<Atomic> atoms = query.selectAtoms();

        for(Atomic atom: atoms){
            Set<Map<String, Concept>> subAnswers = resolveAtomicQuery(atom);
            answers = joinSubstitutions(answers, subAnswers);
        }
        return filterAnswers(query, answers);
    }

    private void expandAtomicQuery(Atomic atom, Set<Query> subGoals, Map<String, Type> varMap) {
        Query query = new Query(atom);
        boolean queryAdmissible = true;
        Iterator<Query> it = subGoals.iterator();
        while( it.hasNext() && queryAdmissible)
            queryAdmissible = !query.isEquivalent(it.next());

        if(queryAdmissible) {
            Set<Rule> rules = getAtomChildren(atom);
            for (Rule rule : rules) {
                Query qr = applyRuleToAtom(atom, rule, varMap);

                //go through each unified atom
                Set<Atomic> atoms = qr.getAtoms();
                atoms.forEach(a -> {
                    if (isAtomRecursive(atom, graph))
                        subGoals.add(query);
                    expandAtomicQuery(a, subGoals, varMap);
                });
            }
        }
    }

    private void expandQuery(Query query, Map<String, Type> varMap) {
        Set<Atomic> atoms = query.getAtoms();

        atoms.forEach(atom -> {
            Set<Query> subGoals = new HashSet<>();
            expandAtomicQuery(atom, subGoals, varMap);
        });
    }

    /**
     * Resolve a given query string using the rule base
     * @param inputQuery the query string to be expanded
     * @return set of answers
     */
    public Set<Map<String, Concept>> resolve(MatchQuery inputQuery) {
        Query query = new Query(inputQuery, graph);
        return resolveQuery(query);
    }

    /**
     * Expand a given query string using the rule base
     * @param inputQuery the query string to be expanded
     * @return expanded query string
     */
    public MatchQuery expand(MatchQuery inputQuery) {
        Query query = new Query(inputQuery, graph);
        Map<String, Type> varMap = query.getVarTypeMap();
        expandQuery(query, varMap);
        return query.getExpandedMatchQuery();
    }

}