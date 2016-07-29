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

package io.mindmaps.reasoner;

import com.google.common.collect.Sets;
import com.google.common.collect.Lists;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.exceptions.MindmapsValidationException;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.RoleType;
import io.mindmaps.core.model.Rule;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.api.parser.QueryParser;
import io.mindmaps.graql.api.query.MatchQuery;
import io.mindmaps.reasoner.internal.container.Query;
import io.mindmaps.reasoner.internal.predicate.Atomic;
import io.mindmaps.reasoner.internal.predicate.RelationAtom;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static io.mindmaps.reasoner.internal.Utility.*;


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

    private boolean checkChildApplicableToAtomThruRelation(RelationAtom parentAtom, Query parent, Query childLHS, Query childRHS, Type relType)
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

    private boolean checkChildApplicableThruRelation(Query parent, Query childLHS, Query childRHS, Type relType)
    {
        boolean relRelevant = false;
        Set<Atomic> relevantAtoms = parent.getAtomsWithType(relType);
        Iterator<Atomic> it = relevantAtoms.iterator();
        while(it.hasNext() && !relRelevant)
            relRelevant = checkChildApplicableToAtomThruRelation((RelationAtom) it.next(), parent, childLHS, childRHS, relType);

        return relRelevant;
    }

    private boolean checkChildApplicableToAtomThruResource(Atomic parent, Query childLHS, Query childRHS, Type type)
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

    private boolean checkChildApplicableThruResource(Query parent, Query childLHS, Query childRHS, Type type)
    {
        boolean resourceApplicable = false;

        Set<Atomic> atoms = parent.getAtomsWithType(type);
        Iterator<Atomic> it = atoms.iterator();
        while(it.hasNext() && !resourceApplicable)
            resourceApplicable = checkChildApplicableToAtomThruResource(it.next(), childLHS, childRHS, type);

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
                        ruleRelevant = checkChildApplicableToAtomThruResource(atom,
                                workingMemory.get(rule.getId()), new Query(rule.getRHS(), graph), type);
            else if (atom.isRelation()) {
                System.out.println("Checking relevance of rule " + rule.getId());
                ruleRelevant = checkChildApplicableToAtomThruRelation((RelationAtom) atom, parent,
                        workingMemory.get(rule.getId()), new Query(rule.getRHS(), graph), type);
                if (!ruleRelevant)
                    System.out.println("Rule " + rule.getId() + " not relevant through type " + type.getId());
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
                    ruleRelevant = checkChildApplicableThruResource(workingMemory.get(parent.getId()),
                            workingMemory.get(rule.getId()), new Query(rule.getRHS(), graph), type);
                else if (type.isRelationType())
                    ruleRelevant = checkChildApplicableThruRelation(workingMemory.get(parent.getId()),
                            workingMemory.get(rule.getId()), new Query(rule.getRHS(), graph), type);

                if (!rule.equals(parent) && ruleRelevant ) children.add(rule);
                else{
                    System.out.println("in getRuleChildren: Rule " + rule.getId() + " not relevant to type " + type.getId() + " " + ruleRelevant);
                }
            }
        }

        return children;
    }

    private void linkConceptTypes(Rule rule)
    {
        System.out.println("Linking rule " + rule.getId() + "...");
        MatchQuery qLHS = qp.parseMatchQuery(rule.getLHS()).getMatchQuery();
        MatchQuery qRHS = qp.parseMatchQuery(rule.getRHS()).getMatchQuery();

        Set<Type> hypothesisConceptTypes = qLHS.admin().getTypes();
        Set<Type> conclusionConceptTypes = qRHS.admin().getTypes();

        hypothesisConceptTypes.forEach(rule::addHypothesis);

        conclusionConceptTypes.forEach(rule::addConclusion);

        System.out.println("Rule " + rule.getId() + " linked");

    }

    private Set<Rule> getRules()
    {
        Set<Rule> rules = new HashSet<>();
        MatchQuery sq = qp.parseMatchQuery("match $x isa inference-rule;").getMatchQuery();

        List<Map<String, Concept>> results = Lists.newArrayList(sq);
        Iterator<Map<String, Concept>> it = results.iterator();

        while( it.hasNext() )
        {
            Map<String, Concept> result = it.next();
            for (Map.Entry<String, Concept> entry : result.entrySet() ) {
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

        System.out.println(rules.size() + " rules initialized...");

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


    private void makeChildRelationConsistent(RelationAtom childAtom, Query childLHS, RelationAtom parentAtom, Query parentLHS,
                                             Map<String, Type> globalVarMap) {

        Set<String> varsToAllocate = parentAtom.getVarNames();
        Set<String> chRelVars = childAtom.getVarNames();
        Set<String> pVars = parentLHS.getVarSet();

        /**construct mapping between child and parent variables*/
        Map<String, String> varMapping = new HashMap<>();
        Map<String, Pair<Type, RoleType>> childMap = childAtom.getVarTypeRoleMap();
        Map<RoleType, Pair<String, Type>> parentMap = parentAtom.getRoleVarTypeMap();

        for (String chVar : chRelVars) {
            RoleType role = childMap.containsKey(chVar)? childMap.get(chVar).getValue1(): null;
            String pVar = role != null && parentMap.containsKey(role) ? parentMap.get(role).getValue0() : "";
            if (pVar.isEmpty())
                pVar = varsToAllocate.iterator().next();

            if ( !chVar.equals(pVar))
                varMapping.put(chVar, pVar);

            varsToAllocate.remove(pVar);
        }

        /**
         * apply variable change from constructed mapping
         */
        Map<String, String> appliedMappings = new HashMap<>();
        for (Map.Entry<String, String> mapping: varMapping.entrySet())
        {
            String varToReplace = mapping.getKey();
            String replacementVar = mapping.getValue();

            if(!appliedMappings.containsKey(varToReplace) || !appliedMappings.get(varToReplace).equals(replacementVar)) {
                /**bidirectional mapping*/
                if (varMapping.containsKey(replacementVar) && varMapping.get(replacementVar).equals(varToReplace))
                {
                    childLHS.exchangeRelVarNames(varToReplace, replacementVar);
                    appliedMappings.put(varToReplace, replacementVar);
                    appliedMappings.put(replacementVar, varToReplace);
                }
                else
                {
                    childLHS.changeRelVarName(varToReplace, replacementVar);
                    appliedMappings.put(varToReplace, replacementVar);
                }
            }
        }

        /**check variables not present in relation*/
        Set<String> modChVars = childLHS.getVarSet();
        Set<String> pRelVars = parentAtom.getVarNames();
        pRelVars.forEach(chRelVar -> modChVars.remove(chRelVar));
        for( String chVar : modChVars)
        {
            if (pVars.contains(chVar) || globalVarMap.containsKey(chVar))
                childLHS.changeVarName(chVar, chVar + chVar.replace("$", ""));
        }
    }

    private void makeChildConsistent(Atomic childAtom, Query childLHS, Atomic parentAtom, Query parentLHS,
                                        Type type, Map<String, Type> globalVarMap)
    {

        if (type.isRelationType())
            makeChildRelationConsistent((RelationAtom) childAtom, childLHS, (RelationAtom) parentAtom, parentLHS, globalVarMap);
        else {
            Set<String> chVars = childLHS.getVarSet();
            Set<String> pVars = parentLHS.getVarSet();

            String parentVar = parentAtom.getVarName();
            String childVar = childAtom.getVarName();
            //if variables not equal do a var exchange
            if (!parentVar.equals(childVar)) {
                //if from exists in child, create a new variable for from
                if (chVars.contains(parentVar)) {
                    String replacement = parentVar + parentVar.replace("$", "");
                    System.out.println("Replacing: " + parentVar + "->" + replacement);
                    childLHS.changeVarName(parentVar, replacement);
                }

                System.out.println("Replacing: " + childVar + "->" + parentVar);
                childLHS.changeVarName(childVar, parentVar);
            }

            //if any of remaining child vars are contained in parent or top, create a new var
            for (String var : chVars) {
                if (!var.equals(parentVar) && ( pVars.contains(var) || globalVarMap.containsKey(var))) {
                    String replacement = var + var.replace("$", "");
                    System.out.println("Replacing: " + var + "->" + replacement);
                    childLHS.changeVarName(var, replacement);
                }
            }
        }
    }

    private Query applyRuleToAtom(Atomic parentAtom, Query parent, Rule child, Map<String, Type> varMap)
    {
        Type type = getRuleConclusionType(child);
        Query childLHS = new Query(child.getLHS(), child, graph);
        Query childRHS = new Query(child.getRHS(), graph);

        Atomic childAtom = getRuleConclusionAtom(childLHS, childRHS, type);
        makeChildConsistent(childAtom, childLHS, parentAtom, parent, type, varMap);
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

        System.out.println("EXPANDED: Parent\n" + parent.toString() + "\nEXPANDED by " + child.getId() + " through type " + type.getId());

        return expansions;
    }

    private void expandQueryWithStack(Query q, Map<String, Type> varMap)
    {
        Stack<Rule> ruleStack = new Stack<>();
        Stack<Query> queryStack = new Stack<>();

        Set<Rule> topRules = getQueryChildren(q);
        topRules.forEach(r -> {
            ruleStack.push(r);
            queryStack.push(q);
        });

        while(!queryStack.isEmpty())
        {
            Query top = queryStack.peek();
            Rule rule = ruleStack.peek();

            Set<Query> expansions = applyRuleToQuery(top, rule, varMap);
            if(!expansions.isEmpty())
            {
                queryStack.pop();
                ruleStack.pop();
                for (Query exp : expansions) {
                    Set<Rule> children = getQueryChildren(exp);
                    children.forEach(r -> {
                        ruleStack.push(r);
                        queryStack.push(exp);
                    });
                }
            }
        }
    }

    private void expandQueryQSQ(Query query, Map<String, Type> varMap)
    {
        System.out.println("expandQueryQSQ: " + (query.getRule() != null ? query.getRule().getId() : "top"));
        for(Atomic atom : query.getAtoms())
        {
            Set<Rule> rules = getAtomChildren(atom, query);

            if(isAtomRecursive(atom, graph))
            {
                System.out.println("Atomic : " + atom.toString() + " is recursive");
                for(Rule r : rules)
                {
                    System.out.println("Attempting expansion by rule + " + r.getId());
                    if (isRuleRecursive(r)) {
                        Query topQuery = query.getTopQueryWithRule(r);
                        boolean ruleApplied = topQuery != null;
                        if (topQuery == null) topQuery = query;

                        MatchQuery Q = topQuery.getExpandedMatchQuery();
                        Set<Map<String, Concept>> Ans = Sets.newHashSet(Q.distinct());

                        Query qr = applyRuleToAtom(atom, query, r, varMap);
                        MatchQuery Qstar = topQuery.getExpandedMatchQuery();
                        Set<Map<String, Concept>> AnsStar = Sets.newHashSet(Qstar.distinct());


                        if ( Ans.size() != AnsStar.size() || !ruleApplied)
                            expandQueryQSQ(qr, varMap);
                        else
                            query.removeExpansionFromAtom(atom, qr);
                    }
                    else{
                        Query qr = applyRuleToAtom(atom, query, r, varMap);
                        expandQueryQSQ(qr, varMap);
                    }
                }

            }
            else {
                for (Rule r : rules) {
                    Query qr = applyRuleToAtom(atom, query, r, varMap);
                    expandQueryQSQ(qr, varMap);
                }
            }

        }

    }

    /**
     * Expand a given query string using the rule base
     * @param inputQuery the query string to be expanded
     * @return expanded query string
     */
    public MatchQuery expandQuery(MatchQuery inputQuery)
    {

        Query query = new Query(inputQuery, graph);
        Map<String, Type> varMap = query.getVarTypeMap();

        expandQueryQSQ(query, varMap);

        MatchQuery expandedQuery = query.getExpandedMatchQuery();
        System.out.println("DNF size: " + expandedQuery.admin().getPattern().getDisjunctiveNormalForm().getPatterns().size());
        return expandedQuery;

    }

}