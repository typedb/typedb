/*
 * Copyright (C) 2021 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.logic;

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Label;
import grakn.core.concept.ConceptManager;
import grakn.core.graph.GraphManager;
import grakn.core.graph.common.Encoding;
import grakn.core.graph.structure.RuleStructure;
import grakn.core.logic.tool.TypeResolver;
import grakn.core.traversal.TraversalEngine;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.variable.ThingVariable;

public class LogicManager {

    private final GraphManager graphMgr;
    private final TypeResolver typeResolver;
    private LogicCache logicCache;

    public LogicManager(GraphManager graphMgr, ConceptManager conceptMgr, TraversalEngine traversalEng, LogicCache logicCache) {
        this.graphMgr = graphMgr;
        this.logicCache = logicCache;
        this.typeResolver = new TypeResolver(conceptMgr, traversalEng, logicCache);
    }

    public Rule putRule(String label, Conjunction<? extends Pattern> when, ThingVariable<?> then) {
        RuleStructure structure = graphMgr.schema().rules().get(label);
        if (structure != null) {
            // overwriting a rule means we purge it and re-create the rule
            structure.delete();
            logicCache.rule().invalidate(label);
        }
        return logicCache.rule().get(label, l -> Rule.of(graphMgr, this, label, when, then));
    }

    public Rule getRule(String label) {
        Rule rule = logicCache.rule().getIfPresent(label);
        if (rule != null) return rule;
        RuleStructure structure = graphMgr.schema().rules().get(label);
        if (structure != null) return logicCache.rule().get(structure.label(), l -> Rule.of(this, structure));
        return null;
    }

    public ResourceIterator<Rule> rules() {
        return graphMgr.schema().rules().all().map(this::fromStructure);
    }

    public ResourceIterator<Rule> rulesConcluding(Label type) {
        return graphMgr.schema().rules().conclusions().concludesVertex(graphMgr.schema().getType(type)).map(this::fromStructure);
    }

    public ResourceIterator<Rule> rulesConcludingHas(Label attributeType) {
        return graphMgr.schema().rules().conclusions().concludesEdgeTo(graphMgr.schema().getType(attributeType)).map(this::fromStructure);
    }


    /**
     * On commit we must clear the rule cache and revalidate rules - this will force re-running type resolution
     * when we re-load the Rule objects
     * Rule indexes should also be deleted and regenerated as needed
     * Note: does not need to be synchronized as only called by one schema transaction at a time
     */
    public void revalidateAndReindexRules() {
        logicCache.rule().clear();

        // validate all rules are valid and satisfiable
        rules().forEachRemaining(Rule::validateSatisfiable);

        // re-index if rules are valid and satisfiable
        if (graphMgr.schema().rules().conclusions().isOutdated()) {
            graphMgr.schema().rules().all().forEachRemaining(s -> fromStructure(s).reindex());
        }

        // using the new index, validate new rules are stratifiable (eg. do not cause cycles through a negation)
        graphMgr.schema().rules().buffered().filter(structure -> structure.status().equals(Encoding.Status.BUFFERED))
                .forEachRemaining(structure -> getRule(structure.label()).validateCycles());
    }

    public TypeResolver typeResolver() {
        return typeResolver;
    }

    GraphManager graph() { return graphMgr; }

    private Rule fromStructure(RuleStructure ruleStructure) {
        return logicCache.rule().get(ruleStructure.label(), l -> Rule.of(this, ruleStructure));
    }

}
