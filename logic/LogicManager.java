/*
 * Copyright (C) 2020 Grakn Labs
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

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Label;
import grakn.core.concept.ConceptManager;
import grakn.core.graph.GraphManager;
import grakn.core.graph.structure.RuleStructure;
import grakn.core.logic.tool.TypeHinter;
import grakn.core.traversal.TraversalEngine;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.variable.ThingVariable;

import java.util.List;

public class LogicManager {

    private final ConceptManager conceptMgr;
    private final GraphManager graphMgr;
    private final TypeHinter typeHinter;
    private LogicCache logicCache;

    public LogicManager(GraphManager graphMgr, ConceptManager conceptMgr, TraversalEngine traversalEng, LogicCache logicCache) {
        this.graphMgr = graphMgr;
        this.conceptMgr = conceptMgr;
        this.typeHinter = new TypeHinter(conceptMgr, traversalEng, logicCache.hinter());
        this.logicCache = logicCache;
    }

    public Rule putRule(String label, Conjunction<? extends Pattern> when, ThingVariable<?> then) {
        RuleStructure structure = graphMgr.schema().getRule(label);
        if (structure != null) {
            structure.delete();
            logicCache.rule().invalidate(label);
        }

        Rule rule = Rule.of(graphMgr, conceptMgr, this, label, when, then);
        logicCache.rule().put(label, rule);

        validateRuleCycles();

        return rule;
    }

    public void validateRuleCycles() {
        List<Rule> uncommittedRulesInCycles = rules().filter(rule -> !rule.isCommitted())
                .filter(this::inNegatedRuleCycle)
                .toList();
        if (!uncommittedRulesInCycles.isEmpty()) {
            throw GraknException.of(ErrorMessage.RuleWrite.RULES_IN_NEGATED_CYCLE_NOT_STRATIFIABLE.message(uncommittedRulesInCycles));
        }
    }

    private boolean inNegatedRuleCycle(Rule rule) {
        // TODO detect negated cycles in the rule graph
        // TODO use the new rule as a starting point
        return false;
    }

    public Rule getRule(String label) {
        Rule rule = logicCache.rule().getIfPresent(label);
        if (rule != null) return rule;
        RuleStructure structure = graphMgr.schema().getRule(label);
        if (structure != null) {
            rule = Rule.of(conceptMgr, this, structure);
            logicCache.rule().put(rule.getLabel(), rule);
            return rule;
        }
        return null;
    }

    public ResourceIterator<Rule> rules() {
        return graphMgr.schema().rules().map(structure -> {
            Rule rule = logicCache.rule().getIfPresent(structure.label());
            if (rule == null) {
                rule = Rule.of(conceptMgr, this, structure);
                logicCache.rule().put(rule.getLabel(), rule);
            }
            return rule;
        });
    }

    public TypeHinter typeHinter() {
        return typeHinter;
    }

}
