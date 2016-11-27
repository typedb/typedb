/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql;

import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graql.internal.reasoner.query.ReasonerMatchQuery;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import com.google.common.collect.Sets;
import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.query.AtomicMatchQuery;
import ai.grakn.graql.internal.reasoner.query.AtomicQuery;
import ai.grakn.graql.internal.reasoner.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class Reasoner {

    private final GraknGraph graph;
    private final Logger LOG = LoggerFactory.getLogger(Reasoner.class);

    public Reasoner(GraknGraph graph) {
        this.graph = graph;
        linkConceptTypes();
    }

    private void commitGraph() {
        try {
            graph.commit();
        } catch (GraknValidationException e) {
            LOG.debug(e.getMessage());
        }
    }

    private void linkConceptTypes(Rule rule) {
        QueryBuilder qb = graph.graql();
        MatchQuery qLHS = qb.match(rule.getLHS());
        MatchQuery qRHS = qb.match(rule.getRHS());

        //TODO fix this hack
        Set<Type> hypothesisConceptTypes = qLHS.admin().getTypes()
            .stream().filter(type -> !type.isRoleType()).collect(Collectors.toSet());
        Set<Type> conclusionConceptTypes = qRHS.admin().getTypes()
            .stream().filter(type -> !type.isRoleType()).collect(Collectors.toSet());

        hypothesisConceptTypes.forEach(rule::addHypothesis);
        conclusionConceptTypes.forEach(rule::addConclusion);
    }

    public static Set<Rule> getRules(GraknGraph graph) {
        return new HashSet<>(graph.getMetaRuleInference().instances());
    }

    /**
     * Link all unlinked rules in the rule base to their matching types
     */
    public void linkConceptTypes() {
        Set<Rule> rules = getRules(graph);
        LOG.debug(rules.size() + " rules initialized...");
        Set<Rule> linkedRules = new HashSet<>();
        rules.stream()
                .filter(rule -> rule.getHypothesisTypes().isEmpty() && rule.getConclusionTypes().isEmpty())
                .forEach(rule -> {
                    linkConceptTypes(rule);
                    linkedRules.add(rule);
                });
        if(!linkedRules.isEmpty()) commitGraph();
        LOG.debug(linkedRules.size() + " rules linked...");
    }

    private void propagateAnswers(Map<AtomicQuery, AtomicQuery> matAnswers){
        matAnswers.keySet().forEach(aq -> {
            if (aq.getParent() == null) aq.propagateAnswers(matAnswers);
        });
    }

    private void recordAnswers(AtomicQuery atomicQuery, Map<AtomicQuery, AtomicQuery> matAnswers) {
        AtomicQuery equivalentQuery = matAnswers.get(atomicQuery);
        if (equivalentQuery != null) {
            QueryAnswers unifiedAnswers = QueryAnswers.getUnifiedAnswers(equivalentQuery, atomicQuery, atomicQuery.getAnswers());
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
            extraSubs.forEach(sub -> newAns.put(sub.getVarName(), graph.getConcept(sub.getPredicateValue())) );
            newAnswers.add(newAns);
        });

        return newAnswers;
    }

    private QueryAnswers answerWM(AtomicQuery atomicQuery, Set<AtomicQuery> subGoals, Map<AtomicQuery, AtomicQuery> matAnswers){
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
                AtomicQuery childAtomicQuery = new AtomicMatchQuery(atIt.next(), atomicQuery.getSelectedNames());
                QueryAnswers subs = answerWM(childAtomicQuery, subGoals, matAnswers);
                while(atIt.hasNext()){
                    childAtomicQuery = new AtomicMatchQuery(atIt.next(), atomicQuery.getSelectedNames());
                    QueryAnswers localSubs = answerWM(childAtomicQuery, subGoals, matAnswers);
                    subs = subs.join(localSubs);
                }

                QueryAnswers answers = propagateHeadIdPredicates(atomicQuery, ruleHead, subs)
                        .filterVars(ruleHead.getSelectedNames())
                        .filterKnown(atomicQuery.getAnswers());
                QueryAnswers newAnswers = new QueryAnswers();
                newAnswers.addAll(new AtomicMatchQuery(ruleHead, answers).materialise());

                if (!newAnswers.isEmpty()) answers = newAnswers;
                //TODO do all combinations if roles missing
                QueryAnswers filteredAnswers = answers
                        .filterVars(atomicQuery.getSelectedNames())
                        .filterIncomplete(atomicQuery.getSelectedNames());
                atomicQuery.getAnswers().addAll(filteredAnswers);
                recordAnswers(atomicQuery, matAnswers);
            }
        }
        else
            atomicQuery.memoryLookup(matAnswers);

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
                AtomicQuery childAtomicQuery = new AtomicMatchQuery(at, atomicQuery.getSelectedNames());
                atomicQuery.establishRelation(childAtomicQuery);
                QueryAnswers subs = answer(childAtomicQuery, subGoals, matAnswers);
                while (atIt.hasNext()) {
                    at = atIt.next();
                    childAtomicQuery = new AtomicMatchQuery(at, atomicQuery.getSelectedNames());
                    atomicQuery.establishRelation(childAtomicQuery);
                    QueryAnswers localSubs = answer(childAtomicQuery, subGoals, matAnswers);
                    subs = subs.join(localSubs);
                }

                QueryAnswers answers = propagateHeadIdPredicates(atomicQuery, ruleHead, subs)
                        .filterVars(atomicQuery.getSelectedNames())
                        .filterKnown(atomicQuery.getAnswers());;
                QueryAnswers newAnswers = new QueryAnswers();
                if (atom.isResource()
                        || atom.isUserDefinedName() && atom.isRelation() )
                    newAnswers.addAll(new AtomicMatchQuery(ruleHead, answers).materialise());
                if (!newAnswers.isEmpty()) answers = answers.join(newAnswers);

                QueryAnswers filteredAnswers = answers.filterVars(atomicQuery.getSelectedNames())
                        .filterIncomplete(atomicQuery.getSelectedNames());
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
            answerWM(atomicQuery, subGoals, matAnswers);
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
        if (!query.isRuleResolvable())
            return new QueryAnswers(Sets.newHashSet(query.execute()));
        Iterator<Atom> atIt = query.selectAtoms().iterator();
        AtomicQuery atomicQuery = new AtomicMatchQuery(atIt.next(), query.getSelectedNames());
        QueryAnswers answers = resolveAtomicQuery(atomicQuery, materialise);
        while(atIt.hasNext()){
            atomicQuery = new AtomicMatchQuery(atIt.next(), query.getSelectedNames());
            QueryAnswers subAnswers = resolveAtomicQuery(atomicQuery, materialise);
            answers = answers.join(subAnswers);
        }
        return answers.filterVars(query.getSelectedNames());
    }

    public void precomputeInferences(){
        Map<AtomicQuery, AtomicQuery> matAnswers = new HashMap<>();
        Set<AtomicQuery> subGoals = new HashSet<>();
        getRules(graph).forEach(rl -> {
            InferenceRule rule = new InferenceRule(rl, graph);
            AtomicQuery atomicQuery = new AtomicMatchQuery(rule.getHead(), new QueryAnswers());
            int dAns;
            Set<AtomicQuery> SG;
            do {
                SG = new HashSet<>(subGoals);
                dAns = atomicQuery.getAnswers().size();
                answer(atomicQuery, SG, matAnswers, true);
                LOG.debug("Atom: " + atomicQuery.getAtom() + " answers: " + atomicQuery.getAnswers().size());
                dAns = atomicQuery.getAnswers().size() - dAns;
            } while (dAns != 0);
            subGoals.addAll(SG);
        });
        commitGraph();
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
        if(materialise) commitGraph();
        return answers;
    }

    public QueryAnswers resolve(MatchQuery inputQuery) { return resolve(inputQuery, false);}

    /**
     * Resolve a given query using the rule base
     * @param inputQuery the query string to be expanded
     * @return MatchQuery with answers
     */
    public MatchQuery resolveToQuery(MatchQuery inputQuery, boolean materialise) {
        if (Reasoner.getRules(graph).isEmpty())
            return inputQuery;
        else {
            MatchQuery outputQuery = new ReasonerMatchQuery(inputQuery, graph, resolve(inputQuery, materialise));
            if (materialise) commitGraph();
            return outputQuery;
        }
    }

    /**
     * Materialised by default, BC reasoning
     * @param inputQuery
     * @return query with answers
     */
    public MatchQuery resolveToQuery(MatchQuery inputQuery) { return resolveToQuery(inputQuery, true);}
}