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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graql.internal.reasoner.query.QueryCache;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.util.Schema;
import com.google.common.collect.Maps;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.var;

public class Reasoner {

    private final GraknGraph graph;
    private static final Logger LOG = LoggerFactory.getLogger(Reasoner.class);

    public Reasoner(GraknGraph graph) {
        this.graph = graph;
        linkConceptTypes(graph);
    }
    private static void commitGraph(GraknGraph graph) {
        try {
            graph.commit();
        } catch (GraknValidationException e) {
            LOG.error(e.getMessage());
        }
    }
    private static void linkConceptTypes(GraknGraph graph, Rule rule) {
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
        return new HashSet<>(graph.admin().getMetaRuleInference().instances());
    }

    public static boolean hasRules(GraknGraph graph) {
        String inferenceRule = Schema.MetaSchema.INFERENCE_RULE.getName();
        return graph.graql().infer(false).match(var("x").isa(inferenceRule)).ask().execute();
    }

    /**
     * Link all unlinked rules in the rule base to their matching types
     */
    public static void linkConceptTypes(GraknGraph graph) {
        Set<Rule> rules = getRules(graph);
        LOG.debug(rules.size() + " rules initialized...");
        Set<Rule> linkedRules = new HashSet<>();
        rules.stream()
                .filter(rule -> rule.getHypothesisTypes().isEmpty() && rule.getConclusionTypes().isEmpty())
                .forEach(rule -> {
                    linkConceptTypes(graph, rule);
                    linkedRules.add(rule);
                });
        if(!linkedRules.isEmpty()) commitGraph(graph);
        LOG.debug(linkedRules.size() + " rules linked...");
    }

    /**
     * materialise all possible inferences
     */
    public void precomputeInferences(){
        linkConceptTypes(graph);
        QueryCache cache = new QueryCache();
        Set<ReasonerAtomicQuery> subGoals = new HashSet<>();
        getRules(graph).forEach(rl -> {
            InferenceRule rule = new InferenceRule(rl, graph);
            ReasonerAtomicQuery atomicQuery = new ReasonerAtomicQuery(rule.getHead());
            int dAns;
            Set<ReasonerAtomicQuery> SG;
            do {
                SG = new HashSet<>(subGoals);
                dAns = atomicQuery.getAnswers().size();
                atomicQuery.answer(SG, cache, true);
                LOG.debug("Atom: " + atomicQuery.getAtom() + " answers: " + atomicQuery.getAnswers().size());
                dAns = atomicQuery.getAnswers().size() - dAns;
            } while (dAns != 0);
            subGoals.addAll(SG);
        });
        commitGraph(graph);
    }

    /**
     * Resolve a given query using the rule base
     * @param inputQuery the query string to be expanded
     * @return stream of answers
     */
    public Stream<Map<String, Concept>> resolve(MatchQuery inputQuery, boolean materialise) {
        Reasoner.linkConceptTypes(graph);
        if (!Reasoner.hasRules(graph)) return inputQuery.stream();
        Iterator<Conjunction<VarAdmin>> conjIt = inputQuery.admin().getPattern().getDisjunctiveNormalForm().getPatterns().iterator();
        ReasonerQuery conjunctiveQuery = new ReasonerQueryImpl(conjIt.next(), graph);
        Stream<Map<String, Concept>> answerStream = conjunctiveQuery.resolve(materialise);
        while(conjIt.hasNext()) {
            conjunctiveQuery = new ReasonerQueryImpl(conjIt.next(), graph);
            Stream<Map<String, Concept>> localStream = conjunctiveQuery.resolve(materialise);
            answerStream = Stream.concat(answerStream, localStream);
        }
        return answerStream.map(result -> Maps.filterKeys(result, inputQuery.admin().getSelectedNames()::contains));
    }
}