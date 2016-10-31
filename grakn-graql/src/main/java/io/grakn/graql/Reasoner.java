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

package io.grakn.graql;

import com.google.common.collect.Lists;
import io.grakn.MindmapsGraph;
import io.grakn.concept.Concept;
import io.grakn.concept.RoleType;
import io.grakn.concept.Rule;
import io.grakn.concept.Type;
import io.grakn.graql.internal.reasoner.atom.Atom;
import io.grakn.graql.internal.reasoner.atom.Predicate;
import io.grakn.graql.internal.reasoner.query.AtomicMatchQuery;
import io.grakn.graql.internal.reasoner.query.AtomicQuery;
import io.grakn.graql.internal.reasoner.query.QueryAnswers;
import io.grakn.graql.internal.reasoner.query.ReasonerMatchQuery;
import io.grakn.graql.internal.reasoner.rule.InferenceRule;
import io.grakn.graql.internal.reasoner.query.Query;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static io.grakn.graql.internal.reasoner.query.QueryAnswers.getUnifiedAnswers;

public class Reasoner {

    private final MindmapsGraph graph;
    private final Logger LOG = LoggerFactory.getLogger(Reasoner.class);

    private final Map<String, InferenceRule> workingMemory = new HashMap<>();

    public Reasoner(MindmapsGraph graph) {
        this.graph = graph;
        linkConceptTypes();
    }

    private boolean checkRuleApplicableToAtom(Atom parentAtom, InferenceRule child) {
        boolean relRelevant = true;
        Query parent = parentAtom.getParentQuery();
        Atom childAtom = child.getRuleConclusionAtom();

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
                            String chId = child.getBody().getIdPredicate(chVar);
                            String pId = parent.getIdPredicate(pVar);
                            if (!chId.isEmpty() && !pId.isEmpty())
                                relRelevant &= chId.equals(pId);
                        }
                    }
                }
            }
        }
        else if (parentAtom.isResource()) {
            String childVal = child.getHead().getValuePredicate(childAtom.getValueVariable());
            String parentVal = parent.getValuePredicate(parentAtom.getValueVariable());
            relRelevant = parentVal.isEmpty() || parentVal.equals(childVal);
        }
        return relRelevant;
    }

    //TODO move to Atomic?
    private Set<Rule> getApplicableRules(Atom atom) {
        Set<Rule> children = new HashSet<>();
        Type type = atom.getType();
        //TODO change if we allow for Types having null type
        if (type == null) {
            Collection<Rule> applicableRules = Reasoner.getRules(graph).stream()
                    .filter(rule -> rule.getConclusionTypes().stream().filter(Type::isRelationType).count() != 0)
                    .collect(Collectors.toSet());
            children.addAll(applicableRules);
        }
        else{
            Collection<Rule> rulesFromType = type.getRulesOfConclusion();
            rulesFromType.forEach(rule -> {
                InferenceRule child = workingMemory.get(rule.getId());
                boolean ruleRelevant = checkRuleApplicableToAtom(atom, child);
                if (ruleRelevant) children.add(rule);
            });
        }
        return children;
    }

    private void linkConceptTypes(Rule rule) {
        LOG.debug("Linking rule " + rule.getId() + "...");
        QueryBuilder qb = Graql.withGraph(graph);
        MatchQuery qLHS = qb.match(qb.parsePatterns(rule.getLHS()));
        MatchQuery qRHS = qb.match(qb.parsePatterns(rule.getRHS()));

        Set<Type> hypothesisConceptTypes = qLHS.admin().getTypes();
        Set<Type> conclusionConceptTypes = qRHS.admin().getTypes();

        hypothesisConceptTypes.forEach(rule::addHypothesis);
        conclusionConceptTypes.forEach(rule::addConclusion);

        LOG.debug("Rule " + rule.getId() + " linked");
    }

    public static Set<Rule> getRules(MindmapsGraph graph) {
        Set<Rule> rules = new HashSet<>();
        QueryBuilder qb = Graql.withGraph(graph);
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
        Set<Rule> rules = getRules(graph);
        LOG.debug(rules.size() + " rules initialized...");

        for (Rule rule : rules) {
            workingMemory.putIfAbsent(rule.getId(), new InferenceRule(rule, graph));
            if (rule.getHypothesisTypes().isEmpty() && rule.getConclusionTypes().isEmpty()) {
                linkConceptTypes(rule);
            }
        }
    }

    private void propagateAnswers(Map<AtomicQuery, AtomicQuery> matAnswers){
        matAnswers.keySet().forEach(aq -> {
            if (aq.getParent() == null) aq.propagateAnswers(matAnswers);
        });
    }

    private void recordAnswers(AtomicQuery atomicQuery, Map<AtomicQuery, AtomicQuery> matAnswers) {
        AtomicQuery equivalentQuery = matAnswers.get(atomicQuery);
        if (equivalentQuery != null) {
            QueryAnswers unifiedAnswers = getUnifiedAnswers(equivalentQuery, atomicQuery, atomicQuery.getAnswers());
            matAnswers.get(atomicQuery).getAnswers().addAll(unifiedAnswers);
        }
        else
            matAnswers.put(atomicQuery, atomicQuery);
    }

    private QueryAnswers propagateHeadIdPredicates(Query atomicQuery, Query ruleHead, QueryAnswers answers){
        QueryAnswers newAnswers = new QueryAnswers();
        if(answers.isEmpty()) return newAnswers;

        Set<String> queryVars = atomicQuery.getSelectedNames();
        Set<String> headVars = ruleHead.getSelectedNames();
        Set<Predicate> extraSubs = new HashSet<>();
        if(queryVars.size() > headVars.size()){
            extraSubs.addAll(ruleHead.getIdPredicates()
                    .stream().filter(sub -> queryVars.contains(sub.getVarName()))
                    .collect(Collectors.toSet()));
        }

        answers.forEach( map -> {
            Map<String, Concept> newAns = new HashMap<>(map);
            extraSubs.forEach(sub -> newAns.put(sub.getVarName(), graph.getInstance(sub.getPredicateValue())) );
            newAnswers.add(newAns);
        });

        return newAnswers;
    }

    private QueryAnswers answerWM(AtomicQuery atomicQuery, Set<AtomicQuery> subGoals){
        boolean queryAdmissible = !subGoals.contains(atomicQuery);
        atomicQuery.DBlookup();

        if(queryAdmissible) {
            Atom atom = atomicQuery.getAtom();
            Set<Rule> rules = getApplicableRules(atom);
            for (Rule rl : rules) {
                InferenceRule rule = new InferenceRule(rl, graph);
                rule.unify(atom);
                Query ruleBody = rule.getBody();
                AtomicQuery ruleHead = rule.getHead();

                Set<Atom> atoms = ruleBody.selectAtoms();
                Iterator<Atom> atIt = atoms.iterator();

                subGoals.add(atomicQuery);
                AtomicQuery childAtomicQuery = new AtomicMatchQuery(atIt.next());
                atomicQuery.establishRelation(childAtomicQuery);
                QueryAnswers subs = answerWM(childAtomicQuery, subGoals);
                while(atIt.hasNext()){
                    childAtomicQuery = new AtomicMatchQuery(atIt.next());
                    atomicQuery.establishRelation(childAtomicQuery);
                    QueryAnswers localSubs = answerWM(childAtomicQuery, subGoals);
                    subs = subs.join(localSubs);
                }
                QueryAnswers answers = propagateHeadIdPredicates(atomicQuery, ruleHead, subs)
                        .filterVars(atomicQuery.getSelectedNames());
                QueryAnswers newAnswers = new QueryAnswers();
                newAnswers.addAll(new AtomicMatchQuery(ruleHead, answers).materialise());
                if (!newAnswers.isEmpty()) answers = newAnswers;
                QueryAnswers filteredAnswers = answers.filterInComplete(atomicQuery.getSelectedNames());
                atomicQuery.getAnswers().addAll(filteredAnswers);
            }
        }
        return atomicQuery.getAnswers();
    }

    private QueryAnswers answer(AtomicQuery atomicQuery, Set<AtomicQuery> subGoals, Map<AtomicQuery, AtomicQuery> matAnswers){
        boolean queryAdmissible = !subGoals.contains(atomicQuery);
        boolean queryVisited = matAnswers.containsKey(atomicQuery);

        if(queryAdmissible) {
            if (!queryVisited){
                atomicQuery.DBlookup();
                recordAnswers(atomicQuery, matAnswers);
            }
            else
                atomicQuery.memoryLookup(matAnswers);

            Atom atom = atomicQuery.getAtom();
            Set<Rule> rules = getApplicableRules(atom);
            for (Rule rl : rules) {
                InferenceRule rule = new InferenceRule(rl, graph);
                rule.unify(atom);
                Query ruleBody = rule.getBody();
                AtomicQuery ruleHead = rule.getHead();

                Set<Atom> atoms = ruleBody.selectAtoms();
                Iterator<Atom> atIt = atoms.iterator();

                subGoals.add(atomicQuery);
                Atom at = atIt.next();
                AtomicQuery childAtomicQuery = new AtomicMatchQuery(at);
                atomicQuery.establishRelation(childAtomicQuery);
                QueryAnswers subs = answer(childAtomicQuery, subGoals, matAnswers);
                while(atIt.hasNext()){
                    at = atIt.next();
                    childAtomicQuery = new AtomicMatchQuery(at);
                    atomicQuery.establishRelation(childAtomicQuery);
                    QueryAnswers localSubs = answer(childAtomicQuery, subGoals, matAnswers);
                    subs = subs.join(localSubs);
                }

                QueryAnswers answers = propagateHeadIdPredicates(atomicQuery, ruleHead, subs)
                        .filterVars(atomicQuery.getSelectedNames());
                QueryAnswers newAnswers = new QueryAnswers();
                if (atom.isResource())
                    newAnswers.addAll(new AtomicMatchQuery(ruleHead, answers).materialise());
                if (!newAnswers.isEmpty()) answers = newAnswers;

                QueryAnswers filteredAnswers = answers.filterInComplete(atomicQuery.getSelectedNames());
                atomicQuery.getAnswers().addAll(filteredAnswers);
                recordAnswers(atomicQuery, matAnswers);
            }
        }
        else
            atomicQuery.memoryLookup(matAnswers);

        return atomicQuery.getAnswers();
    }

    private void answer(AtomicQuery atomicQuery, Set<AtomicQuery> subGoals, Map<AtomicQuery, AtomicQuery> matAnswers,
                        boolean materialise){
        if(!materialise) {
            answer(atomicQuery, subGoals, matAnswers);
            propagateAnswers(matAnswers);
        }
        else
            answerWM(atomicQuery, subGoals);
    }

    private QueryAnswers resolveAtomicQuery(AtomicQuery atomicQuery, boolean materialise) {
        int dAns;
        int iter = 0;

        if (!atomicQuery.getAtom().isRuleResolvable()){
            atomicQuery.DBlookup();
            return atomicQuery.getAnswers();
        }
        else {
            Map<AtomicQuery, AtomicQuery> matAnswers = new HashMap<>();
            if (!materialise) {
                atomicQuery.DBlookup();
                recordAnswers(atomicQuery, matAnswers);
                matAnswers.put(atomicQuery, atomicQuery);
            }
            do {
                Set<AtomicQuery> subGoals = new HashSet<>();
                dAns = atomicQuery.getAnswers().size();
                answer(atomicQuery, subGoals, matAnswers, materialise);
                LOG.debug("iter: " + iter++ + " answers: " + atomicQuery.getAnswers().size());
                dAns = atomicQuery.getAnswers().size() - dAns;
            } while (dAns != 0);
            return atomicQuery.getAnswers();
        }
    }

    private QueryAnswers resolveQuery(Query query, boolean materialise) {
        Iterator<Atom> atIt = query.selectAtoms().iterator();
        AtomicQuery atomicQuery = new AtomicMatchQuery(atIt.next());
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