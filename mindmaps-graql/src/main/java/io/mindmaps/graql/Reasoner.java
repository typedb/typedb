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
import io.mindmaps.concept.*;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.query.Patterns;
import io.mindmaps.graql.internal.reasoner.rule.InferenceRule;
import io.mindmaps.graql.internal.reasoner.query.*;
import io.mindmaps.graql.internal.reasoner.predicate.Atomic;
import io.mindmaps.graql.internal.reasoner.query.Query;
import io.mindmaps.util.ErrorMessage;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static io.mindmaps.graql.internal.reasoner.Utility.createFreshVariable;
import static io.mindmaps.graql.internal.reasoner.Utility.createRelationVar;

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

    /**
     * create transitive rule R(from: X, to: Y) :- R(from: X,to: Z), R(from: Z, to: Y)
     * @param ruleId rule identifier
     * @param relType transitive relation type
     * @param from from directional role type
     * @param to to directional role type
     * @param graph graph
     * @return
     */
    public static Rule createTransitiveRule(String ruleId, RelationType relType, RoleType from, RoleType to, MindmapsGraph graph){
        final int arity = relType.hasRoles().size();
        if (arity != 2)
            throw new IllegalArgumentException(ErrorMessage.RULE_CREATION_ARITY_ERROR.getMessage());

        Var startVar = Graql.var().isa(relType.getId()).rel(from.getId(), "x").rel(to.getId(), "z");
        Var endVar = Graql.var().isa(relType.getId()).rel(from.getId(), "z").rel(to.getId(), "y");
        Var headVar = Graql.var().isa(relType.getId()).rel(from.getId(), "x").rel(to.getId(), "y");

        String body = Graql.match(startVar, endVar).select("x", "y").toString();
        String head = Graql.match(headVar).select("x", "y").toString();
        return graph.putRule(ruleId, body, head, graph.getMetaRuleInference());
    }

    /**
     * creates rule parent :- child
     * @param ruleId rule identifier
     * @param parent relation type of parent
     * @param child relation type of child
     * @param roleMappings map of corresponding role types
     * @param graph graph
     * @return
     */
    public static Rule createSubPropertyRule(String ruleId, RelationType parent, RelationType child, Map<RoleType, RoleType> roleMappings,
                                             MindmapsGraph graph){
        final int parentArity = parent.hasRoles().size();
        final int childArity = child.hasRoles().size();
        if (parentArity != childArity || parentArity != roleMappings.size())
            throw new IllegalArgumentException(ErrorMessage.RULE_CREATION_ARITY_ERROR.getMessage());

        Var parentVar = Graql.var().isa(parent.getId());
        Var childVar = Graql.var().isa(child.getId());
        Set<String> vars = new HashSet<>();
        roleMappings.forEach( (parentRole, childRole) -> {
            String varName = createFreshVariable(vars, "x");
            parentVar.rel(parentRole.getId(), varName);
            childVar.rel(childRole.getId(), varName);
            vars.add(varName);
        });

        String body = Graql.match(childVar).toString();
        String head = Graql.match(parentVar).toString();
        return graph.putRule(ruleId, body, head, graph.getMetaRuleInference());
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
                            String chVal = child.getBody().getValue(chVar);
                            String pVal = parent.getValue(pVar);
                            if (!chVal.isEmpty() && !pVal.isEmpty())
                                relRelevant &= chVal.equals(pVal);
                        }
                    }
                }
            }
        }
        else if (parentAtom.isResource())
            relRelevant = parentAtom.getVal().equals(childAtom.getVal());

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
        MatchQuery qLHS = qb.parseMatch(rule.getLHS());
        MatchQuery qRHS = qb.parseMatch(rule.getRHS());

        Set<Type> hypothesisConceptTypes = qLHS.admin().getTypes();
        Set<Type> conclusionConceptTypes = qRHS.admin().getTypes();

        hypothesisConceptTypes.forEach(rule::addHypothesis);
        conclusionConceptTypes.forEach(rule::addConclusion);

        LOG.debug("Rule " + rule.getId() + " linked");

    }

    public Set<Rule> getRules() {
        Set<Rule> rules = new HashSet<>();
        MatchQuery sq = qb.parseMatch("match $x isa inference-rule;");

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

    private QueryAnswers answer(AtomicQuery atomicQuery, Set<AtomicQuery> subGoals, Map<AtomicQuery, QueryAnswers> matAnswers) {
        Atomic atom = atomicQuery.getAtom();

        atomicQuery.DBlookup();
        atomicQuery.memoryLookup(matAnswers);

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
                Atomic at = atIt.next();
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

                QueryAnswers answers = propagateHeadSubstitutions(atomicQuery, ruleHead, subs);
                QueryAnswers filteredAnswers = answers.filter(atomicQuery.getSelectedNames());
                atomicQuery.getAnswers().addAll(filteredAnswers);

                recordAnswers(atomicQuery, matAnswers);
            }
        }

        return atomicQuery.getAnswers();
    }

    private QueryAnswers resolveAtomicQuery(AtomicQuery atomicQuery) {
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
                LOG.debug("iter: " + iter++ + " answers: " + dAns);
                answer(atomicQuery, subGoals, matAnswers);
                propagateAnswers(matAnswers);
                dAns = atomicQuery.getAnswers().size() - dAns;
            } while (dAns != 0);
            return atomicQuery.getAnswers();
        }

    }

    private QueryAnswers resolveQuery(Query query, boolean materialize) {
        Iterator<Atomic> atIt = query.selectAtoms().iterator();

        AtomicQuery atomicQuery = new AtomicMatchQuery(atIt.next());
        QueryAnswers answers = resolveAtomicQuery(atomicQuery);
        if(materialize) answers.materialize(atomicQuery);
        while(atIt.hasNext()){
            atomicQuery = new AtomicMatchQuery(atIt.next());
            QueryAnswers subAnswers = resolveAtomicQuery(atomicQuery);
            if(materialize) subAnswers.materialize(atomicQuery);
            answers = answers.join(subAnswers);
        }

        return answers.filter(query.getSelectedNames());
    }

    /**
     * Resolve a given query using the rule base
     * @param inputQuery the query string to be expanded
     * @return set of answers
     */
    public QueryAnswers resolve(MatchQuery inputQuery) {
        Query query = new ReasonerMatchQuery(inputQuery, graph);
        return resolveQuery(query, false);
    }

    public MatchQuery resolveToQuery(MatchQuery inputQuery) {
        Query query = new Query(inputQuery, graph);
        if (!query.isRuleResolvable()) return inputQuery;
        QueryAnswers answers = resolveQuery(query, false);
        return new ReasonerMatchQuery(inputQuery, graph, answers);
    }
}