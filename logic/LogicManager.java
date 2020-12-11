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

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Label;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.type.RelationType;
import grakn.core.graph.GraphManager;
import grakn.core.graph.structure.RuleStructure;
import grakn.core.graph.util.Encoding;
import grakn.core.logic.tool.TypeHinter;
import grakn.core.traversal.TraversalEngine;
import graql.lang.pattern.Conjunctable;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Negation;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.constraint.TypeConstraint;
import graql.lang.pattern.variable.BoundVariable;
import graql.lang.pattern.variable.ThingVariable;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.common.exception.ErrorMessage.RuleWrite.TYPES_NOT_FOUND;

public class LogicManager {

    private final ConceptManager conceptMgr;
    private final GraphManager graphMgr;
    private final TypeHinter typeHinter;
    private LogicCache logicCache;

    public LogicManager(GraphManager graphMgr, ConceptManager conceptMgr, TraversalEngine traversalEng, LogicCache logicCache) {
        this.graphMgr = graphMgr;
        this.conceptMgr = conceptMgr;
        this.logicCache = logicCache;
        this.typeHinter = new TypeHinter(conceptMgr, traversalEng, logicCache);
    }

    public Rule putRule(String label, Conjunction<? extends Pattern> when, ThingVariable<?> then) {
        RuleStructure structure = graphMgr.schema().getRule(label);
        if (structure != null) {
            // overwriting a rule means we purge it and re-create the rule
            structure.delete();
            logicCache.rule().invalidate(label);
        }
        return logicCache.rule().get(label, l -> Rule.of(graphMgr, conceptMgr, this, label, when, then));
    }

    public Rule getRule(String label) {
        Rule rule = logicCache.rule().getIfPresent(label);
        if (rule != null) return rule;
        RuleStructure structure = graphMgr.schema().getRule(label);
        if (structure != null) {
            return logicCache.rule().get(structure.label(), l -> Rule.of(this, structure));
        }
        return null;
    }

    public ResourceIterator<Rule> rules() {
        return graphMgr.schema().rules().map(struct -> {
            Rule rule = logicCache.rule().getIfPresent(struct.label());
            if (rule == null) rule = logicCache.rule().get(struct.label(), l -> Rule.of(this, struct));
            return rule;
        });
    }

    /**
     * On commit we must clear the rule cache and revalidate rules
     * Rule indexes should also be deleted and regenerated at approximate the same time
     * Note: does not need to by synchronized as only called by one schema transaction at a time
     */
    public void validateRules() {
        logicCache.rule().clear();
        // validate all schema structures contain valid types
        graphMgr.schema().rules().forEachRemaining(structure -> validateRuleStructureLabels(conceptMgr, structure));
        // validate all rules are satisfiable
        rules().forEachRemaining(Rule::validateSatisfiable);
        // validate new rules are stratifiable (eg. do not cause cycles through a negation)
        graphMgr.schema().bufferedRules().filter(structure -> structure.status().equals(Encoding.Status.BUFFERED))
                .forEach(structure -> getRule(structure.label()).validateCycles());
    }

    TypeHinter typeHinter() {
        return typeHinter;
    }

    static void validateRuleStructureLabels(ConceptManager conceptMgr, RuleStructure ruleStructure) {
        graql.lang.pattern.Conjunction<Conjunctable> whenNormalised = ruleStructure.when().normalise().patterns().get(0);
        Stream<BoundVariable> positiveVariables = whenNormalised.patterns().stream().filter(Conjunctable::isVariable)
                .map(Conjunctable::asVariable);
        Stream<BoundVariable> negativeVariables = whenNormalised.patterns().stream().filter(Conjunctable::isNegation)
                .flatMap(p -> negationVariables(p.asNegation()));
        Stream<Label> whenPositiveLabels = getTypeLabels(positiveVariables);
        Stream<Label> whenNegativeLabels = getTypeLabels(negativeVariables);
        Stream<Label> thenLabels = getTypeLabels(ruleStructure.then().variables());
        Set<String> invalidLabels = invalidLabels(conceptMgr, Stream.of(whenPositiveLabels, whenNegativeLabels, thenLabels).flatMap(Function.identity()));
        if (!invalidLabels.isEmpty()) {
            throw GraknException.of(TYPES_NOT_FOUND, ruleStructure.label(), String.join(", ", invalidLabels));
        }
    }

    private static Stream<Label> getTypeLabels(Stream<BoundVariable> variables) {
        return variables.filter(BoundVariable::isType).map(variable -> variable.asType().label())
                .filter(Optional::isPresent).map(labelConstraint -> {
                    TypeConstraint.Label label = labelConstraint.get();
                    if (label.scope().isPresent()) return Label.of(label.label(), label.scope().get());
                    else return Label.of(label.label());
                });
    }

    private static Stream<BoundVariable> negationVariables(Negation<?> ruleNegation) {
        assert ruleNegation.patterns().size() == 1 && ruleNegation.patterns().get(0).isDisjunction();
        return ruleNegation.patterns().get(0).asDisjunction().patterns().stream()
                .flatMap(pattern -> pattern.asConjunction().patterns().stream()).map(Pattern::asVariable);
    }

    private static Set<String> invalidLabels(ConceptManager conceptMgr, Stream<Label> labels) {
        return labels.filter(label -> {
            if (label.scope().isPresent()) {
                RelationType scope = conceptMgr.getRelationType(label.scope().get());
                if (scope == null) return false;
                return scope.getRelates(label.name()) == null;
            } else {
                return conceptMgr.getType(label.name()) == null;
            }
        }).map(Label::scopedName).collect(Collectors.toSet());
    }
}
