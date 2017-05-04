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
import ai.grakn.graql.Graql;
import ai.grakn.util.Schema;

import java.util.HashSet;
import java.util.Set;

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

}