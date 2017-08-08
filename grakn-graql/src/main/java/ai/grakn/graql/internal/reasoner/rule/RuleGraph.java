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

package ai.grakn.graql.internal.reasoner.rule;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Label;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.Rule;
import ai.grakn.graql.Graql;
import ai.grakn.util.Schema;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.var;

public class RuleGraph {

    private final GraknGraph graph;

    public RuleGraph(GraknGraph graph){
        this.graph = graph;
    }

    /**
     * @return set of inference rule contained in the graph
     */
    public Set<Rule> getRules() {
        return new HashSet<>(graph.admin().getMetaRuleInference().instances());
    }

    /**
     * @return true if at least one inference rule is present in the graph
     */
    public boolean hasRules() {
        Label inferenceRule = Schema.MetaSchema.INFERENCE_RULE.getLabel();
        return graph.graql().infer(false).match(var("x").isa(Graql.label(inferenceRule))).ask().execute();
    }

    /**
     * @param type for which rules containing it in the head are sought
     * @return rules containing specified type in the head
     */
    public Set<Rule> getRulesWithType(OntologyConcept type){
        return type != null ?
                type.subs().stream().flatMap(t -> t.getRulesOfConclusion().stream()).collect(Collectors.toSet()) :
                getRules();
    }

    /**
     * @return
     */
    public boolean hasLoops(){
        return getRules().stream()
                .flatMap(rule -> rule.getConclusionTypes().stream())
                .distinct()
                .filter(type -> !type.getRulesOfHypothesis().isEmpty())
                .findFirst().isPresent();
    }

    /**
     * @return
     */
    public boolean hasTypesWithNegativeFlux(){
        return getRules().stream()
                .flatMap(rule -> rule.getConclusionTypes().stream())
                .distinct()
                .filter(type -> type.getRulesOfConclusion().size() > type.getRulesOfHypothesis().size())
                .findFirst().isPresent();
    }

    /**
     * @return
     */
    public boolean hasRulesGeneratingFreshVariables(){
        return getRules().stream()
                .map(r -> new InferenceRule(r, graph))
                .filter(InferenceRule::generatesFreshVariables)
                .findFirst().isPresent();
    }

    /**
     *
     * @return
     */
    public boolean requiresReiteration(){
        return hasTypesWithNegativeFlux() || hasRulesGeneratingFreshVariables();
    }


}
