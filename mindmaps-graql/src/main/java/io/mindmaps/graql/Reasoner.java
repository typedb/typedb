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
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Rule;
import io.mindmaps.concept.Type;
import io.mindmaps.graql.internal.reasoner.query.AtomicMatchQuery;
import io.mindmaps.graql.internal.reasoner.query.AtomicQuery;
import io.mindmaps.graql.internal.reasoner.query.QueryAnswers;
import io.mindmaps.graql.internal.reasoner.query.ReasonerMatchQuery;
import io.mindmaps.graql.internal.reasoner.rule.InferenceRule;
import io.mindmaps.graql.internal.reasoner.predicate.Atomic;
import io.mindmaps.graql.internal.reasoner.query.Query;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class Reasoner {

    private final MindmapsGraph graph;
    private final QueryBuilder qb;
    private final Logger LOG = LoggerFactory.getLogger(Reasoner.class);

    private final Map<String, InferenceRule> workingMemory = new HashMap<>();

    public Reasoner(MindmapsGraph graph) {
        this.graph = graph;
        qb = Graql.withGraph(graph);
        linkConceptTypes();
    }

    private boolean checkRuleApplicableToAtom(Atomic parentAtom, InferenceRule child) {
        boolean relRelevant = true;
        Query parent = parentAtom.getParentQuery();
        Atomic childAtom = child.getRuleConclusionAtom();

        if (parentAtom.isRelation()) {
            Map<RoleType, Pair<String, Type>> childRoleVarTypeMap = childAtom.getRoleVarTypeMap();
            //Check for role compatibility
            Map<RoleType, Pair<String, Type>> parentRoleVarTypeMap = parentAtom.getRoleVarTypeMap();
            for (Map.Entry<RoleType, Pair<String, Type>> entry : parentRoleVarTypeMap.entrySet()) {
                RoleType role = entry.getKey();
                Type pType = entry.getValue().getValue();
                if (pType != null) {
                    //vars can be matched by role types
                    if (childRoleVarTypeMap.containsKey(role)) {
                        Type chType = childRoleVarTypeMap.get(role).getValue();
                        //check type compatibility
                        if (chType != null) {
                            relRelevant &= pType.equals(chType) || chType.subTypes().contains(pType);

                            //Check for any constraints on the variables
                            String chVar = childRoleVarTypeMap.get(role).getKey();
                            String pVar = entry.getValue().getKey();
                            String chId = child.getBody().getSubstitution(chVar);
                            String pId = parent.getSubstitution(pVar);
                            if (!chId.isEmpty() && !pId.isEmpty())
                                relRelevant &= chId.equals(pId);
                        }
                    }
                }
            }
        }
        else if (parentAtom.isResource()) {
            String childVal = child.getHead().getValuePredicate(childAtom.getVal());
            String parentVal = parent.getValuePredicate(parentAtom.getVal());
            relRelevant = parentVal.isEmpty() || parentVal.equals(childVal);
        }

        return relRelevant;
    }

    private Set<Rule> getApplicableRules(Atomic atom) {
        Set<Rule> children = new HashSet<>();

        String typeId = atom.getTypeId();
        if (typeId.isEmpty()) return children;
        Type type = graph.getType(typeId);

        Collection<Rule> rulesFromType = type.getRulesOfConclusion();

        rulesFromType.forEach( rule -> {
            InferenceRule child = workingMemory.get(rule.getId());
            boolean ruleRelevant = checkRuleApplicableToAtom(atom, child);
            if (ruleRelevant) children.add(rule);
        });

        return children;
    }

    private void linkConceptTypes(Rule rule) {
        LOG.debug("Linking rule " + rule.getId() + "...");
        MatchQuery qLHS = qb.parse(rule.getLHS());
        MatchQuery qRHS = qb.parse(rule.getRHS());

        Set<Type> hypothesisConceptTypes = qLHS.admin().getTypes();
        Set<Type> conclusionConceptTypes = qRHS.admin().getTypes();

        hypothesisConceptTypes.forEach(rule::addHypothesis);
        conclusionConceptTypes.forEach(rule::addConclusion);

        LOG.debug("Rule " + rule.getId() + " linked");

    }

    public Set<Rule> getRules() {
        Set<Rule> rules = new HashSet<>();
        MatchQuery sq = qb.parse("match $x isa inference-rule;");
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

        for (Rule rule : rules) {
            workingMemory.putIfAbsent(rule.getId(), new InferenceRule(rule, graph));
            if (rule.getHypothesisTypes().isEmpty() && rule.getConclusionTypes().isEmpty()) {
                linkConceptTypes(rule);
            }
        }
    }

    private void propagateAnswers(Map<AtomicQuery, QueryAnswers> matAnswers){
        matAnswers.keySet().forEach( aq -> {
           if (aq.getParent() == null) aq.propagateAnswers(matAnswers);
        });
    }

    private void recordAnswers(AtomicQuery atomicQuery, Map<AtomicQuery, QueryAnswers> matAnswers) {
        if (matAnswers.keySet().contains(atomicQuery))
            matAnswers.get(atomicQuery).addAll(atomicQuery.getAnswers());
        else
            matAnswers.put(atomicQuery, atomicQuery.getAnswers());
    }

    private QueryAnswers propagateHeadSubstitutions(Query atomicQuery, Query ruleHead, QueryAnswers answers){
        QueryAnswers newAnswers = new QueryAnswers();
        if(answers.isEmpty()) return newAnswers;

        Set<String> queryVars = atomicQuery.getSelectedNames();
        Set<String> headVars = ruleHead.getSelectedNames();
        Set<Atomic> extraSubs = new HashSet<>();
        if(queryVars.size() > headVars.size()){
            extraSubs.addAll(ruleHead.getSubstitutions()
                    .stream().filter(sub -> queryVars.contains(sub.getVarName()))
                    .collect(Collectors.toSet()));
        }

        answers.forEach( map -> {
            Map<String, Concept> newAns = new HashMap<>(map);
            extraSubs.forEach(sub -> newAns.put(sub.getVarName(), graph.getInstance(sub.getVal())) );
            newAnswers.add(newAns);
        });

        return newAnswers;
    }

    private QueryAnswers answer(AtomicQuery atomicQuery, Set<AtomicQuery> subGoals, Map<AtomicQuery,
            QueryAnswers> matAnswers, boolean materialise) {
        Atomic atom = atomicQuery.getAtom();
        atomicQuery.DBlookup();
        if(!materialise) atomicQuery.memoryLookup(matAnswers);

        boolean queryAdmissible = !subGoals.contains(atomicQuery);
        if(queryAdmissible) {
            Set<Rule> rules = getApplicableRules(atom);
            for (Rule rl : rules) {
                InferenceRule rule = new InferenceRule(rl, graph);
                rule.unify(atom);
                Query ruleBody = rule.getBody();
                AtomicQuery ruleHead = rule.getHead();

                Set<Atomic> atoms = ruleBody.selectAtoms();
                Iterator<Atomic> atIt = atoms.iterator();

                subGoals.add(atomicQuery);
                AtomicQuery childAtomicQuery = new AtomicMatchQuery(atIt.next());
                atomicQuery.establishRelation(childAtomicQuery);
                QueryAnswers subs = answer(childAtomicQuery, subGoals, matAnswers, materialise);
                while(atIt.hasNext()){
                    childAtomicQuery = new AtomicMatchQuery(atIt.next());
                    atomicQuery.establishRelation(childAtomicQuery);
                    QueryAnswers localSubs = answer(childAtomicQuery, subGoals, matAnswers, materialise);
                    subs = subs.join(localSubs);
                }
                QueryAnswers answers = propagateHeadSubstitutions(atomicQuery, ruleHead, subs)
                                            .filterVars(atomicQuery.getSelectedNames());
                QueryAnswers newAnswers = new QueryAnswers();
                if (materialise || atom.isResource())
                    newAnswers.addAll(new AtomicMatchQuery(ruleHead, answers).materialise());
                if (!newAnswers.isEmpty()) answers = newAnswers;

                QueryAnswers filteredAnswers = answers.filterInComplete(atomicQuery.getSelectedNames());
                atomicQuery.getAnswers().addAll(filteredAnswers);
                if (!materialise) recordAnswers(atomicQuery, matAnswers);
            }
        }
        return atomicQuery.getAnswers();
    }

    private QueryAnswers resolveAtomicQuery(AtomicQuery atomicQuery, boolean materialise) {
        int dAns;
        int iter = 0;

        if (!atomicQuery.getAtom().isRuleResolvable()){
            atomicQuery.DBlookup();
            return atomicQuery.getAnswers();
        }
        else {
            Map<AtomicQuery, QueryAnswers> matAnswers = new HashMap<>();
            matAnswers.put(atomicQuery, atomicQuery.getAnswers());
            do {
                Set<AtomicQuery> subGoals = new HashSet<>();
                dAns = atomicQuery.getAnswers().size();
                answer(atomicQuery, subGoals, matAnswers, materialise);
                propagateAnswers(matAnswers);
                LOG.debug("iter: " + iter++ + " answers: " + atomicQuery.getAnswers().size());
                dAns = atomicQuery.getAnswers().size() - dAns;
            } while (dAns != 0);
            return atomicQuery.getAnswers();
        }
    }

    private QueryAnswers resolveQuery(Query query, boolean materialise) {
        Iterator<Atomic> atIt = query.selectAtoms().iterator();
        AtomicQuery atomicQuery = new AtomicMatchQuery(atIt.next().clone());
        QueryAnswers answers = resolveAtomicQuery(atomicQuery, materialise);
        while(atIt.hasNext()){
            atomicQuery = new AtomicMatchQuery(atIt.next());
            QueryAnswers subAnswers = resolveAtomicQuery(atomicQuery, materialise);
            answers = answers.join(subAnswers);
        }
        return answers.filterVars(query.getSelectedNames());
    }

    /**
     * Resolve a given query using the rule base
     * @param inputQuery the query string to be expanded
     * @return set of answers
     */
    public QueryAnswers resolve(MatchQuery inputQuery, boolean materialise) {
        Query query = new ReasonerMatchQuery(inputQuery, graph);
        return resolveQuery(query, materialise);
    }

    public QueryAnswers  resolve(MatchQuery inputQuery) { return resolve(inputQuery, false);}

    /**
     * Resolve a given query using the rule base
     * @param inputQuery the query string to be expanded
     * @return MatchQuery with answers
     */
    public MatchQuery resolveToQuery(MatchQuery inputQuery, boolean materialise) {
        Query query = new Query(inputQuery, graph);
        if (!query.isRuleResolvable()) return inputQuery;
        QueryAnswers answers = resolveQuery(query, materialise);
        return new ReasonerMatchQuery(inputQuery, graph, answers);
    }

    public MatchQuery resolveToQuery(MatchQuery inputQuery) { return resolveToQuery(inputQuery, false);}
}