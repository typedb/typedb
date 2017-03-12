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
import ai.grakn.concept.TypeName;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.reasoner.cache.LazyQueryCache;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.reasoner.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import java.util.Iterator;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.name;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.graql.internal.util.CommonUtil.optionalOr;

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
            graph.commitOnClose();
            graph.close();
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
        TypeName inferenceRule = Schema.MetaSchema.INFERENCE_RULE.getName();
        return graph.graql().infer(false).match(var("x").isa(name(inferenceRule))).ask().execute();
    }

    /**
     * resolve query and provide each answer with a corresponding explicit path
     * @param query to resolve
     * @param materialise whether to materialise inferences
     * @return stream of answers
     */
    public static Stream<Answer> resolveWithExplanation(MatchQuery query, boolean materialise) {
        GraknGraph graph = optionalOr(query.admin().getGraph()).orElseThrow(
                () -> new IllegalStateException(ErrorMessage.NO_GRAPH.getMessage())
        );

        if (!Reasoner.hasRules(graph)) return query.admin().streamWithVarNames().map(QueryAnswer::new);

        Iterator<Conjunction<VarAdmin>> conjIt = query.admin().getPattern().getDisjunctiveNormalForm().getPatterns().iterator();
        ReasonerQuery conjunctiveQuery = new ReasonerQueryImpl(conjIt.next(), graph);
        Stream<Answer> answerStream = conjunctiveQuery.resolve(materialise);
        while(conjIt.hasNext()) {
            conjunctiveQuery = new ReasonerQueryImpl(conjIt.next(), graph);
            Stream<Answer> localStream = conjunctiveQuery.resolve(materialise);
            answerStream = Stream.concat(answerStream, localStream);
        }
        Set<VarName> selectVars = query.admin().getSelectedNames();
        return answerStream.map(result -> result.filterVars(selectVars));
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
                Set<Answer> answers = atomicQuery.answerStream(SG, cache, dCache, true, iter != 0).collect(Collectors.toSet());
                LOG.debug("Atom: " + atomicQuery.getAtom() + " answers: " + answers.size() + " dAns: " + dAns);
                dAns = cache.answerSize(SG) - dAns;
                Reasoner.commitGraph(graph);
                iter++;
            } while (dAns != 0);
            subGoals.addAll(SG);
        });
    }
}