package io.mindmaps.reasoner;


import com.google.common.collect.Lists;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.exceptions.MindmapsValidationException;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.RoleType;
import io.mindmaps.core.model.Rule;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.api.parser.QueryParser;
import io.mindmaps.graql.api.query.MatchQuery;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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

    public void printMatchQueryResults(MatchQuery sq)
    {
        List<Map<String, Concept>> results = Lists.newArrayList(sq);
        Iterator<Map<String, Concept>> it = results.iterator();

        while( it.hasNext() )
        {
            Map<String, Concept> result = it.next();
            for (Map.Entry<String, Concept> entry : result.entrySet() ) {
                Concept concept = entry.getValue();
                System.out.print(entry.getKey() + ": " + concept.getId() + " : " + concept.getValue() + " ");
            }
            System.out.println();
        }
    }

    private boolean checkRelationApplicable(Query parent, Query childLHS, Query childRHS, Type relType)
    {
        boolean relRelevant = true;
        LOG.debug("in checkRelationApplicable: type: " + relType.getId() + " chRHS:\n" + childLHS.toString());

        Atom childAtom = childRHS.getAtomsWithType(relType).iterator().next();
        Set<Atom> relevantAtoms = parent.getAtomsWithType(relType);

        /**Check for role compatibility*/
        Map<RoleType, Pair<String, Type>> childRoleVarTypeMap = childLHS.getRoleVarTypeMap(childAtom);
        for(Atom parentAtom : relevantAtoms) {

            Map<RoleType, Pair<String, Type>> parentRoleVarTypeMap = parent.getRoleVarTypeMap(parentAtom);
            for (Map.Entry<RoleType, Pair<String, Type>> entry : parentRoleVarTypeMap.entrySet()) {
                RoleType role = entry.getKey();
                Type pType = entry.getValue().getValue1();
                /**vars can be matched by role types*/
                if (childRoleVarTypeMap.containsKey(role)) {
                    Type chType = childRoleVarTypeMap.get(role).getValue1();
                    /**check type compatibility*/
                    if (chType != null) {

                        relRelevant &= pType.equals(chType) ||
                                pType.subTypes().contains(chType) || chType.subTypes().contains(pType);

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
        }

        LOG.debug("in checkRelationApplicable: relRelevant: " + relRelevant);
        return relRelevant;

    }

    private boolean checkResourceApplicable(Query parent, Query childRHS, Type type)
    {
        boolean resourceApplicable = false;

        LOG.debug("in checkResourceApplicable: type: " + type.getId());
        LOG.debug("parent:\n" + parent.toString());
        LOG.debug("child:\n" + childRHS.toString());

        Atom childAtom = getRuleConclusionAtom(childRHS, type);
        String childVal = childAtom.getVal();

        Set<Atom> atoms = parent.getAtomsWithType(type);
        for(Atom atom : atoms)
        {
            String atomTypeId = atom.getTypeId();
            if(atomTypeId.equals(type.getId())) {
                String val = atom.getVal();
                resourceApplicable = resourceApplicable || val.equals(childVal);
            }
        }
        return resourceApplicable;

    }

    /**
     * Check whether two types can play a common role in a relation
     * @param pTypeId type id of the parent var
     * @param chTypeId type id of the child var
     * @param relId id of the relation type the variables are present in
     * @return
     */
    private boolean checkTypesCompatible(String pTypeId, String chTypeId, String relId)
    {
        boolean typesCompatible = false;
        if (!chTypeId.equals(pTypeId) && !chTypeId.isEmpty() && !pTypeId.isEmpty())
        {
            Collection<RoleType> relRoles = graph.getRelationType(relId).hasRoles();
            Collection<RoleType> pTypeRoles = graph.getType(pTypeId).playsRoles();
            Collection<RoleType> chTypeRoles = graph.getType(chTypeId).playsRoles();
            for (RoleType rt : relRoles)
                typesCompatible = typesCompatible|| (pTypeRoles.contains(rt) && chTypeRoles.contains(rt));
        }

        return typesCompatible;
    }

    private Type getRuleConclusionType(Rule rule)
    {
        Set<Type> types = new HashSet<>();
        Collection<Type> unfilteredTypes = rule.getConclusionTypes();
        for(Type type : unfilteredTypes)
            if (!type.isRoleType()) types.add(type);

        if (types.size() > 1)
            throw new IllegalArgumentException("Found more than single conclusion type!");

        return types.iterator().next();
    }

    private Atom getRuleConclusionAtom(Query ruleRHS, Type type)
    {
        Set<Atom> atoms = ruleRHS.getAtomsWithType(type);
        if (atoms.size() > 1)
            throw new IllegalArgumentException("Found more than single relevant conclusion atom!");

        return atoms.iterator().next();
    }

    private Set<Rule> getQueryChildren(Query query)
    {
        Set<Rule> children = new HashSet<>();
        MatchQuery parent = query.getMatchQuery();
        Set<Type> types = parent.admin().getTypes();
        for( Type type : types)
        {
            Collection<Rule> rulesFromType = type.getRulesOfConclusion();

            for ( Rule rule : rulesFromType) {
                boolean ruleRelevant = true;
                if (type.isResourceType())
                    ruleRelevant = checkResourceApplicable(query, new Query(rule.getRHS(), graph), type);
                else if (type.isRelationType()) {
                    LOG.debug("Checking relevance of rule " + rule.getId());
                    ruleRelevant = checkRelationApplicable(query, workingMemory.get(rule.getId()), new Query(rule.getRHS(), graph), type);
                    if (!ruleRelevant) LOG.debug("Rule " + rule.getId() + " not relevant through type " + type.getId());
                }

                if (ruleRelevant) children.add(rule);
            }
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
                    ruleRelevant = checkResourceApplicable(workingMemory.get(parent.getId()), new Query(rule.getRHS(), graph), type);
                else if (type.isRelationType())
                    ruleRelevant = checkRelationApplicable(workingMemory.get(parent.getId()),
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


    private void makeChildRelationConsistent(Atom childAtom, Query childLHS, Atom parentAtom, Query parentLHS,
                                             Map<String, Type> globalVarMap) {

        Set<String> varsToAllocate = parentAtom.getVarNames();
        Set<String> chRelVars = childAtom.getVarNames();
        Set<String> pVars = parentLHS.getVarSet();
        Set<String> chVars = childLHS.getVarSet();

        /**construct mapping between child and parent variables*/
        Map<String, String> varMapping = new HashMap<>();
        Map<String, Pair<Type, RoleType>> childMap = childLHS.getVarTypeRoleMap(childAtom);
        Map<RoleType, Pair<String, Type>> parentMap = parentLHS.getRoleVarTypeMap(parentAtom);

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
                    childLHS.changeVarName(replacementVar, "$temp");
                    childLHS.changeVarName(varToReplace, replacementVar);
                    childLHS.changeVarName("$temp", varToReplace);
                    appliedMappings.put(varToReplace, replacementVar);
                    appliedMappings.put(replacementVar, varToReplace);
                }
                else
                {
                    childLHS.changeVarName(varToReplace, replacementVar);
                    appliedMappings.put(varToReplace, replacementVar);
                }
            }
        }

        /**check variables not present in relation*/
        chRelVars.forEach(chRelVar -> chVars.remove(chRelVar));
        for( String chVar : chVars)
        {
            if (pVars.contains(chVar) || globalVarMap.containsKey(chVar))
                childLHS.changeVarName(chVar, chVar + chVar.replace("$", ""));
        }
    }

    private void makeChildConsistent(Atom childAtom, Query childLHS, Atom parentAtom, Query parentLHS,
                                        Type type, Map<String, Type> globalVarMap)
    {

        if (type.isRelationType())
            makeChildRelationConsistent(childAtom, childLHS, parentAtom, parentLHS, globalVarMap);
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
                    LOG.debug("Replacing: " + parentVar + "->" + replacement);
                    childLHS.changeVarName(parentVar, replacement);
                }

                LOG.debug("Replacing: " + childVar + "->" + parentVar);
                childLHS.changeVarName(childVar, parentVar);
            }

            //if any of remaining child vars are contained in parent or top, create a new var
            for (String var : chVars) {
                if (!var.equals(parentVar) && ( pVars.contains(var) || globalVarMap.containsKey(var))) {
                    String replacement = var + var.replace("$", "");
                    LOG.debug("Replacing: " + var + "->" + replacement);
                    childLHS.changeVarName(var, replacement);
                }
            }
        }
    }

    private Set<Query> applyRuleToQuery(Query parent, Rule child, Map<String, Type> varMap)
    {
        Type type = getRuleConclusionType(child);
        Set<Query> expansions = new HashSet<>();
        Query childRHS = new Query(child.getRHS(), graph);

        Set<Atom> atoms = parent.getAtomsWithType(type);
        if (atoms == null) return expansions;

        for(Atom atom : atoms)
        {
            Query childLHS = new Query(child.getLHS(), graph);
            Atom childAtom = getRuleConclusionAtom(childRHS, type);

            makeChildConsistent(childAtom, childLHS, atom, parent, type, varMap);
            expansions.add(childLHS);
            parent.expandAtomByQuery(atom, childLHS);
        }

        LOG.debug("EXPANDED: Parent\n" + parent.toString() + "\nEXPANDED by " + child.getId() + " through type " + type.getId());

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

    private void expandQuery(Query q, Map<String, Type> varMap)
    {
        Set<Rule> rules = getQueryChildren(q);
        for( Rule rule : rules )
        {
            Set<Query> expansions = applyRuleToQuery(q, rule, varMap);
            for(Query exp: expansions)
                expandQuery(exp, varMap);
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

        expandQueryWithStack(query, varMap);

        return query.getExpandedMatchQuery();

    }

}