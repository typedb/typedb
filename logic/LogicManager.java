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

import grakn.core.concept.ConceptManager;
import grakn.core.graph.GraphManager;
import grakn.core.graph.structure.RuleStructure;
import grakn.core.logic.tool.TypeHinter;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.variable.ThingVariable;

public class LogicManager {

    private final ConceptManager conceptMgr;
    private final GraphManager graphMgr;
    private final TypeHinter typeHinter;

    public LogicManager(GraphManager graphMgr, ConceptManager conceptMgr, TypeHinter typeHinter) {
        this.graphMgr = graphMgr;
        this.conceptMgr = conceptMgr;
        this.typeHinter = typeHinter;
    }

    public Rule putRule(String label, Conjunction<? extends Pattern> when, ThingVariable<?> then) {
        final RuleStructure structure = graphMgr.schema().getRule(label);
        if (structure != null) structure.delete();
        Rule rule = Rule.of(conceptMgr, graphMgr, typeHinter, label, when, then);

        // TODO detect negated cycles in the rule graph after inserting this rule and updating a rule graph

        return rule;
    }

    public Rule getRule(String label) {
        final RuleStructure structure = graphMgr.schema().getRule(label);
        if (structure != null) return Rule.of(conceptMgr, structure, typeHinter);
        return null;
    }

    public TypeHinter typeHinter() {
        return typeHinter;
    }
}
