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

package ai.grakn.graql.internal.reasoner;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Rule;
import ai.grakn.concept.TypeLabel;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.reasoner.cache.LazyQueryCache;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.util.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.var;

/**
 *
 * <p>
 * Class providing reasoning utility functions.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class Reasoner {

    private static int commitFrequency = 50;
    private static final Logger LOG = LoggerFactory.getLogger(Reasoner.class);

    public static void commitGraph(GraknGraph graph) {
        try {
            graph.commit();
        } catch (GraknValidationException e) {
            LOG.error(e.getMessage());
        }
    }

    public static void setCommitFrequency(int freq){ commitFrequency = freq;}
    public static int getCommitFrequency(){ return commitFrequency;}

    /**
     *
     * @param graph to be checked against
     * @return set of inference rule contained in the graph
     */
    public static Set<Rule> getRules(GraknGraph graph) {
        return new HashSet<>(graph.admin().getMetaRuleInference().instances());
    }

    /**
     *
     * @param graph to be checked against
     * @return true if at least one inference rule is present in the graph
     */
    public static boolean hasRules(GraknGraph graph) {
        TypeLabel inferenceRule = Schema.MetaSchema.INFERENCE_RULE.getLabel();
        return graph.graql().infer(false).match(var("x").isa(Graql.label(inferenceRule))).ask().execute();
    }

    /**
     * materialise all possible inferences
     */
    public static void precomputeInferences(GraknGraph graph){
        LazyQueryCache<ReasonerAtomicQuery> cache = new LazyQueryCache<>();
        LazyQueryCache<ReasonerAtomicQuery> dCache = new LazyQueryCache<>();
        Set<ReasonerAtomicQuery> subGoals = new HashSet<>();
        getRules(graph).forEach(rl -> {
            InferenceRule rule = new InferenceRule(rl, graph);
            ReasonerAtomicQuery atomicQuery = new ReasonerAtomicQuery(rule.getHead());
            int iter = 0;
            long dAns = 0;
            Set<ReasonerAtomicQuery> SG;
            do {
                SG = new HashSet<>(subGoals);
                Set<Answer> answers = atomicQuery.answerStream(SG, cache, dCache, true, false, iter != 0).collect(Collectors.toSet());
                LOG.debug("Atom: " + atomicQuery.getAtom() + " answers: " + answers.size() + " dAns: " + dAns);
                dAns = cache.answerSize(SG) - dAns;
                Reasoner.commitGraph(graph);
                iter++;
            } while (dAns != 0);
            subGoals.addAll(SG);
        });
    }
}