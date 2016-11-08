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
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Rule;
import io.mindmaps.concept.Type;
import io.mindmaps.graql.internal.reasoner.atom.Atom;
import io.mindmaps.graql.internal.reasoner.atom.Predicate;
import io.mindmaps.graql.internal.reasoner.query.AtomicMatchQuery;
import io.mindmaps.graql.internal.reasoner.query.AtomicQuery;
import io.mindmaps.graql.internal.reasoner.query.QueryAnswers;
import io.mindmaps.graql.internal.reasoner.query.ReasonerMatchQuery;
import io.mindmaps.graql.internal.reasoner.rule.InferenceRule;
import io.mindmaps.graql.internal.reasoner.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static io.mindmaps.graql.internal.reasoner.query.QueryAnswers.getUnifiedAnswers;

public class Reasoner {

    private final MindmapsGraph graph;
    private final Logger LOG = LoggerFactory.getLogger(Reasoner.class);

    public Reasoner(MindmapsGraph graph) {
        this.graph = graph;
        linkConceptTypes();
    }

    private void linkConceptTypes(Rule rule) {
        QueryBuilder qb = graph.graql();
        MatchQuery qLHS = qb.match(qb.parsePatterns(rule.getLHS()));
        MatchQuery qRHS = qb.match(qb.parsePatterns(rule.getRHS()));

        Set<Type> hypothesisConceptTypes = qLHS.admin().getTypes();
        Set<Type> conclusionConceptTypes = qRHS.admin().getTypes();

        hypothesisConceptTypes.forEach(rule::addHypothesis);
        conclusionConceptTypes.forEach(rule::addConclusion);
    }

    public static Set<Rule> getRules(MindmapsGraph graph) {
        Set<Rule> rules = new HashSet<>();
        QueryBuilder qb = graph.graql();
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
        rules.stream()
                .filter(rule -> rule.getHypothesisTypes().isEmpty() && rule.getConclusionTypes().isEmpty())
                .forEach(this::linkConceptTypes);
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
            Set<Rule> rules = atom.getApplicableRules();
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
            Set<Rule> rules = atom.getApplicableRules();
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
                while (atIt.hasNext()) {
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
                if (!newAnswers.isEmpty()) answers = answers.join(newAnswers);

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
            do {
                Set<AtomicQuery> subGoals = new HashSet<>();
                dAns = atomicQuery.getAnswers().size();
                answer(atomicQuery, subGoals, matAnswers, materialise);
                LOG.debug("Atom: " + atomicQuery.getAtom() + " iter: " + iter++ + " answers: " + atomicQuery.getAnswers().size());
                dAns = atomicQuery.getAnswers().size() - dAns;
            } while (dAns != 0);
            return atomicQuery.getAnswers();
        }
    }

    private QueryAnswers resolveConjunctiveQuery(Query query, boolean materialise) {
        if (!query.isRuleResolvable()) return new QueryAnswers(Sets.newHashSet(query.execute()));
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
        Set<String> selectVars = inputQuery.admin().getSelectedNames();
        QueryAnswers answers = new QueryAnswers();
        inputQuery.admin().getPattern()
                .getDisjunctiveNormalForm()
                .getPatterns()
                .forEach( conj -> {
                    Query cq = new ReasonerMatchQuery(graph.graql().match(conj).select(selectVars), graph);
                    answers.addAll(resolveConjunctiveQuery(cq, materialise));
                });
        return answers;
    }

    public QueryAnswers  resolve(MatchQuery inputQuery) { return resolve(inputQuery, false);}

    /**
     * Resolve a given query using the rule base
     * @param inputQuery the query string to be expanded
     * @return MatchQuery with answers
     */
    public MatchQuery resolveToQuery(MatchQuery inputQuery, boolean materialise) {
        return new ReasonerMatchQuery(inputQuery, graph, resolve(inputQuery, materialise));
    }

    public MatchQuery resolveToQuery(MatchQuery inputQuery) { return resolveToQuery(inputQuery, false);}
}