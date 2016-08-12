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

import com.google.common.collect.Sets;
import com.google.common.collect.Lists;
import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.core.implementation.MindmapsValidationException;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.RoleType;
import io.mindmaps.core.model.Rule;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.internal.reasoner.container.Query;
import io.mindmaps.graql.internal.reasoner.predicate.Atomic;
import io.mindmaps.graql.internal.reasoner.predicate.RelationAtom;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static io.mindmaps.graql.internal.reasoner.Utility.*;


public class MindmapsReasoner {

    private final MindmapsTransaction graph;
    private final QueryParser qp;
    private final Logger LOG = LoggerFactory.getLogger(MindmapsReasoner.class);

    private final Map<String, Query> workingMemory = new HashMap<>();

    public MindmapsReasoner(MindmapsTransaction tr){
        this.graph = tr;
        qp =  QueryParser.create(graph);
        

        linkConceptTypes();
    }

    private boolean checkChildApplicableToAtomViaRelation(RelationAtom parentAtom, Query parent, Query childLHS, Query childRHS, Type relType)
    {
        boolean relRelevant = true;
        
        Atomic childAtom = getRuleConclusionAtom(childLHS, childRHS, relType);
        Map<RoleType, Pair<String, Type>> childRoleVarTypeMap = ((RelationAtom) childAtom).getRoleVarTypeMap();

        /**Check for role compatibility*/
        Map<RoleType, Pair<String, Type>> parentRoleVarTypeMap = parentAtom.getRoleVarTypeMap();
        for (Map.Entry<RoleType, Pair<String, Type>> entry : parentRoleVarTypeMap.entrySet()) {
            RoleType role = entry.getKey();
            Type pType = entry.getValue().getValue1();
            /**vars can be matched by role types*/
            if (childRoleVarTypeMap.containsKey(role)) {
                Type chType = childRoleVarTypeMap.get(role).getValue1();
                /**check type compatibility*/
                if (chType != null) {

                    relRelevant &= pType.equals(chType) || chType.subTypes().contains(pType);

                    /**Check for any constraints on the variables*/
                    String chVar = childRoleVarTypeMap.get(role).getValue0();
                    String pVar = entry.getValue().getValue0();
                    String chVal = childLHS.getValue(chVar);
                    String pVal = parent.getValue(pVar);
                    if ( !chVal.isEmpty() && !pVal.isEmpty())
                        relRelevant &= chVal.equals(pVal);
                }
            }
        }

        return relRelevant;
    }

    private boolean checkChildApplicableViaRelation(Query parent, Query childLHS, Query childRHS, Type relType)
    {
        boolean relRelevant = false;
        Set<Atomic> relevantAtoms = parent.getAtomsWithType(relType);
        Iterator<Atomic> it = relevantAtoms.iterator();
        while(it.hasNext() && !relRelevant)
            relRelevant = checkChildApplicableToAtomViaRelation((RelationAtom) it.next(), parent, childLHS, childRHS, relType);

        return relRelevant;
    }

    private boolean checkChildApplicableToAtomViaResource(Atomic parent, Query childLHS, Query childRHS, Type type)
    {
        boolean resourceApplicable = false;

        Atomic childAtom = getRuleConclusionAtom(childLHS, childRHS, type);
        String childVal = childAtom.getVal();

        String atomTypeId = parent.getTypeId();
        if(atomTypeId.equals(type.getId())) {
            String val = parent.getVal();
            resourceApplicable = val.equals(childVal);
        }

        return resourceApplicable;
    }

    private boolean checkChildApplicableViaResource(Query parent, Query childLHS, Query childRHS, Type type)
    {
        boolean resourceApplicable = false;

        Set<Atomic> atoms = parent.getAtomsWithType(type);
        Iterator<Atomic> it = atoms.iterator();
        while(it.hasNext() && !resourceApplicable)
            resourceApplicable = checkChildApplicableToAtomViaResource(it.next(), childLHS, childRHS, type);

        return resourceApplicable;

    }

    private Set<Rule> getQueryChildren(Query query)
    {
        Set<Rule> children = new HashSet<>();

        query.getAtoms().stream().filter(Atomic::isType).forEach(atom -> children.addAll(getAtomChildren(atom, query)));

        return children;
    }

    private Set<Rule> getAtomChildren(Atomic atom, Query parent)
    {
        Set<Rule> children = new HashSet<>();

        String typeId = atom.getTypeId();
        if (typeId.isEmpty()) return children;
        Type type = graph.getType(typeId);

        Collection<Rule> rulesFromType = type.getRulesOfConclusion();

        for (Rule rule : rulesFromType)
        {
            boolean ruleRelevant = true;
            if (atom.isResource())
                        ruleRelevant = checkChildApplicableToAtomViaResource(atom,
                                workingMemory.get(rule.getId()), new Query(rule.getRHS(), graph), type);
            else if (atom.isRelation()) {
                LOG.debug("Checking relevance of rule " + rule.getId());
                ruleRelevant = checkChildApplicableToAtomViaRelation((RelationAtom) atom, parent,
                        workingMemory.get(rule.getId()), new Query(rule.getRHS(), graph), type);
                if (!ruleRelevant)
                    LOG.debug("Rule " + rule.getId() + " not relevant through type " + type.getId());
            }

            if (ruleRelevant) children.add(rule);
        }

        return children;
    }


    private Set<Rule> getRuleChildren(Rule parent)
    {
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
        MatchQueryMap qLHS = qp.parseMatchQuery(rule.getLHS()).getMatchQuery();
        MatchQueryMap qRHS = qp.parseMatchQuery(rule.getRHS()).getMatchQuery();

        Set<Type> hypothesisConceptTypes = qLHS.admin().getTypes();
        Set<Type> conclusionConceptTypes = qRHS.admin().getTypes();

        hypothesisConceptTypes.forEach(rule::addHypothesis);

        conclusionConceptTypes.forEach(rule::addConclusion);

        LOG.debug("Rule " + rule.getId() + " linked");

    }

    private Set<Rule> getRules()
    {
        Set<Rule> rules = new HashSet<>();
        MatchQueryMap sq = qp.parseMatchQuery("match $x isa inference-rule;").getMatchQuery();

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
    public void linkConceptTypes()
    {
        Set<Rule> rules = getRules();

        LOG.debug(rules.size() + " rules initialized...");

        for(Rule rule : rules) {
            workingMemory.putIfAbsent(rule.getId(), new Query(rule.getLHS(), graph));
            if(rule.getHypothesisTypes().isEmpty() && rule.getConclusionTypes().isEmpty()) {
                linkConceptTypes(rule);
            }
        }
        try {
            graph.commit();
        } catch (MindmapsValidationException e) {
            e.printStackTrace();
        }
    }

    private void restoreWM()
    {
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
    private String createFreshVariable(Set<String> globalVars, Set<String> childVars, String var)
    {
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
    private void resolveCaptures(Query query, Set<String> globalVars)
    {
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

    /**
     * propagate variables to child via a relation atom (atom variables are bound)
     * @param childAtom child atom being resolved
     * @param childLHS child query
     * @param parentAtom parent atom (predicate) being resolved (subgoal)
     * @param parentLHS parent query
     * @param globalVarMap map containing global vars and their types
     */
    private void propagateVariablesViaRelationAtom(RelationAtom childAtom, Query childLHS, RelationAtom parentAtom, Query parentLHS,
                                             Map<String, Type> globalVarMap) {

        Set<String> varsToAllocate = parentAtom.getVarNames();
        Set<String> childBVs = childAtom.getVarNames();

        /**construct mapping between child and parent bound variables*/
        Map<String, String> varMappings = new HashMap<>();
        Map<String, Pair<Type, RoleType>> childMap = childAtom.getVarTypeRoleMap();
        Map<RoleType, Pair<String, Type>> parentMap = parentAtom.getRoleVarTypeMap();

        for (String chVar : childBVs) {
            RoleType role = childMap.containsKey(chVar)? childMap.get(chVar).getValue1(): null;
            String pVar = role != null && parentMap.containsKey(role) ? parentMap.get(role).getValue0() : "";
            if (pVar.isEmpty())
                pVar = varsToAllocate.iterator().next();

            if ( !chVar.equals(pVar))
                varMappings.put(chVar, pVar);

            varsToAllocate.remove(pVar);
        }

        /**do alpha-conversion*/
        childLHS.changeRelVarNames(varMappings);
        resolveCaptures(childLHS, globalVarMap.keySet());

        /**check free variables for possible captures*/
        Set<String> childFVs = childLHS.getVarSet();
        Set<String> parentBVs = parentAtom.getVarNames();
        Set<String> parentVars = parentLHS.getVarSet();
        parentBVs.forEach(childFVs::remove);

        childFVs.forEach(chVar -> {
            // if (x e P) v (x e G)
            // x -> fresh
                if (parentVars.contains(chVar) || globalVarMap.containsKey(chVar)) {
                    String freshVar = createFreshVariable(globalVarMap.keySet(), childLHS.getVarSet(), chVar);
                    childLHS.changeVarName(chVar, freshVar);
                }
            });

        }

    /**
     * propagate variables to child via a non-relation atom (atom variables are bound)
     * @param childAtom child atom being resolved
     * @param childLHS child query
     * @param parentAtom parent atom (predicate) being resolved (subgoal)
     * @param parentLHS parent query
     * @param globalVarMap map containing global vars and their types
     */
    private void propagateVariablesViaAtom(Atomic childAtom, Query childLHS, Atomic parentAtom, Query parentLHS,
                                                   Map<String, Type> globalVarMap) {

        Set<String> chVars = childLHS.getVarSet();
        Set<String> pVars = parentLHS.getVarSet();

        String parentBV = parentAtom.getVarName();
        String childBV = childAtom.getVarName();
        //if bound vars not equal alpha-convert
        if (!parentBV.equals(childBV)) {
            LOG.debug("Replacing: " + childBV + "->" + parentBV);
            childLHS.changeVarName(childBV, parentBV);
            resolveCaptures(childLHS, globalVarMap.keySet());
        }

        //if any of free vars of child are contained in parent or global, create a new var
        chVars.forEach( var -> {
            // if (x e P) v (x e G)
            // x -> fresh
            if (!var.equals(parentBV) && ( pVars.contains(var) || globalVarMap.containsKey(var))) {
                String freshVar = createFreshVariable(globalVarMap.keySet(), chVars, var);
                LOG.debug("Replacing: " + var + "->" + freshVar);
                childLHS.changeVarName(var, freshVar);
            }
        });

    }

    /**
     * make child query consistent by performing variable substitution so that parent variables are propagated
     * @param childAtom child atom being resolved
     * @param childLHS child query
     * @param parentAtom parent atom (predicate) being resolved (subgoal)
     * @param parentLHS parent query
     * @param type type of the predicate being resolved
     * @param globalVarMap map containing global vars and their types
     */
    private void makeChildConsistent(Atomic childAtom, Query childLHS, Atomic parentAtom, Query parentLHS,
                                        Type type, Map<String, Type> globalVarMap)
    {

        if (type.isRelationType())
            propagateVariablesViaRelationAtom((RelationAtom) childAtom, childLHS, (RelationAtom) parentAtom, parentLHS, globalVarMap);
        else
            propagateVariablesViaAtom(childAtom, childLHS, parentAtom, parentLHS, globalVarMap);

        //update global vars
        Map<String, Type> varTypeMap = childLHS.getVarTypeMap();
        for(Map.Entry<String, Type> entry : varTypeMap.entrySet())
            globalVarMap.putIfAbsent(entry.getKey(), entry.getValue());

    }

    private Query applyRuleToAtom(Atomic parentAtom, Query parent, Rule child, Map<String, Type> varMap)
    {
        Type type = getRuleConclusionType(child);
        Query childLHS = new Query(child.getLHS(), child, graph);
        Query childRHS = new Query(child.getRHS(), graph);

        Atomic childAtom = getRuleConclusionAtom(childLHS, childRHS, type);
        makeChildConsistent(childAtom, childLHS, parentAtom, parent, type, varMap);

        //TODO add parent constraints

        parentAtom.addExpansion(childLHS);

        return childLHS;
    }

    private Set<Query> applyRuleToQuery(Query parent, Rule child, Map<String, Type> varMap)
    {
        Type type = getRuleConclusionType(child);
        Set<Query> expansions = new HashSet<>();

        Set<Atomic> atoms = parent.getAtomsWithType(type);
        if (atoms == null) return expansions;

        atoms.forEach(atom -> expansions.add(applyRuleToAtom(atom, parent, child, varMap)));

        LOG.debug("EXPANDED: Parent\n" + parent.toString() + "\nEXPANDED by " + child.getId() + " through type " + type.getId());

        return expansions;
    }

    /**
     * expand query by performing SLD-AL resolution
     * @param query query to be expanded
     * @param varMap map of global variables with their corresponding types
     */
    private void expandQuery(Query query, Map<String, Type> varMap)
    {
        LOG.debug("expandQuery: " + (query.getRule() != null ? query.getRule().getId() : "top"));
        for(Atomic atom : query.getAtoms())
        {
            Set<Rule> rules = getAtomChildren(atom, query);

            if(isAtomRecursive(atom, graph))
            {
                LOG.debug("Atomic : " + atom.toString() + " is recursive");
                for(Rule r : rules)
                {
                    LOG.debug("Attempting expansion by rule + " + r.getId());
                    if (isRuleRecursive(r)) {
                        Query topQuery = query.getTopQueryWithRule(r);
                        boolean ruleApplied = topQuery != null;
                        if (topQuery == null) topQuery = query;

                        MatchQueryMap Q = topQuery.getExpandedMatchQuery();
                        Set<Map<String, Concept>> Ans = Sets.newHashSet(Q.distinct());

                        Query qr = applyRuleToAtom(atom, query, r, varMap);
                        MatchQueryMap Qstar = topQuery.getExpandedMatchQuery();
                        Set<Map<String, Concept>> AnsStar = Sets.newHashSet(Qstar.distinct());


                        if ( Ans.size() != AnsStar.size() || !ruleApplied)
                            expandQuery(qr, varMap);
                        else
                            query.removeExpansionFromAtom(atom, qr);
                    }
                    else{
                        Query qr = applyRuleToAtom(atom, query, r, varMap);
                        expandQuery(qr, varMap);
                    }
                }

            }
            else {
                for (Rule r : rules) {
                    Query qr = applyRuleToAtom(atom, query, r, varMap);
                    expandQuery(qr, varMap);
                }
            }

        }

    }

    /**
     * Expand a given query string using the rule base
     * @param inputQuery the query string to be expanded
     * @return expanded query string
     */
    public MatchQueryMap expandQuery(MatchQueryMap inputQuery)
    {

        Query query = new Query(inputQuery, graph);
        Map<String, Type> varMap = query.getVarTypeMap();

        expandQuery(query, varMap);

        MatchQueryMap expandedQuery = query.getExpandedMatchQuery();

        LOG.debug("DNF size: " + expandedQuery.admin().getPattern().getDisjunctiveNormalForm().getPatterns().size());

        return expandedQuery;
    }

}