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

package grakn.core.reasoner.inference;

import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Label;
import grakn.core.concept.type.Type;
import grakn.core.concept.type.impl.TypeImpl;
import grakn.core.graph.GraphManager;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.constraint.type.SubConstraint;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.Variable;
import grakn.core.traversal.Identifier;
import graql.lang.common.GraqlArg;
import graql.lang.pattern.variable.Reference;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static graql.lang.common.GraqlToken.Type.ATTRIBUTE;
import static graql.lang.common.GraqlToken.Type.RELATION;
import static graql.lang.common.GraqlToken.Type.ROLE;
import static graql.lang.common.GraqlToken.Type.THING;

public class TypeInference {

    public static void full(Conjunction conjunction, GraphManager graphManager) {
        ConstraintMapper constraintMapper = new ConstraintMapper(conjunction);
        InferenceVariables inferenceVariables = constraintMapper.getInferenceVariables();
        Map<Label, TypeVariable> labelMap = labelVarsFromConjunction(conjunction);
        Map<Reference, Set<Label>> referenceHintsMapping = computeHints(new HashSet<>(inferenceVariables.getInferenceVariables()), graphManager);
        long numOfThings = graphManager.schema().stats().thingTypeCount();
        for (Variable variable : conjunction.variables()) {
            if (variable.reference().isLabel()) continue;
            Set<Label> hintLabels = referenceHintsMapping.get(variable.reference());
            if (variable.isThing()) {
                if (hintLabels.size() != numOfThings) {
                    addInferredIsaLabels(variable.asThing(), referenceHintsMapping.get(variable.reference()), labelMap, graphManager);
                }
                addInferredRoleLabels(variable.asThing(), referenceHintsMapping, inferenceVariables);
            } else if (variable.isType() && hintLabels.size() != numOfThings) {
                addInferredSubLabels(variable.asType(), referenceHintsMapping.get(variable.reference()), labelMap, graphManager);
            }
        }
    }

    public static void simple(Conjunction conjunction, GraphManager graphManager) {
        ConstraintMapper constraintMapper = new ConstraintMapper(conjunction);
        InferenceVariables inferenceVariables = constraintMapper.getInferenceVariables();
        Map<Label, TypeVariable> labelMap = labelVarsFromConjunction(conjunction);
        long numOfThings = graphManager.schema().stats().thingTypeCount();

        for (Variable variable : conjunction.variables()) {
            if (variable.reference().isLabel()) continue;
            Set<Variable> neighbourhood = new HashSet<>();
            TypeVariable typeVariable = inferenceVariables.getConversion(variable);
            neighbourhood.add(typeVariable);
            neighbourhood.addAll(constraintMapper.getVariableNeighbours().get(typeVariable));
            Map<Reference, Set<Label>> localTypeHints = computeHints(neighbourhood, graphManager);
            Set<Label> hintLabels = localTypeHints.get(variable.reference());
            if (variable.isThing()) {
                if (hintLabels.size() != numOfThings) {
                    addInferredIsaLabels(variable.asThing(), localTypeHints.get(variable.reference()), labelMap, graphManager);
                }
                addInferredRoleLabels(variable.asThing(), localTypeHints, inferenceVariables);
            } else if (variable.isType() && hintLabels.size() != numOfThings) {
                addInferredSubLabels(variable.asType(), localTypeHints.get(variable.reference()), labelMap, graphManager);
            }
        }

        ensureHintsConformToTheirSuper(conjunction, graphManager);
    }

    private static void ensureHintsConformToTheirSuper(Conjunction conjunction, GraphManager graphManager) {
        Set<Variable> visited = new HashSet<>();
        conjunction.variables().forEach(variable -> ensureHintsConformToTheirSuper(variable, visited, graphManager));
    }

    private static TypeVariable above(Variable variable) {
        if (variable.isType()) {
            if (variable.asType().sub().isPresent()) return variable.asType().sub().get().type();
            return null;
        } else if (variable.isThing()) {
            if (variable.asThing().isa().isPresent()) return variable.asThing().isa().get().type();
            return null;
        } else throw GraknException.of(ILLEGAL_STATE);
    }

    private static void clearHintLabels(Variable variable) {
        if (variable.isType()) variable.asType().sub().ifPresent(SubConstraint::clearHintLabels);
        else if (variable.isThing()) variable.asThing().isa().ifPresent(IsaConstraint::clearHintLabels);
        else throw GraknException.of(ILLEGAL_STATE);
    }

    private static void removeHintLabel(Variable variable, Label label) {
        if (variable.isType()) variable.asType().sub().ifPresent(subConstraint -> subConstraint.removeHint(label));
        else if (variable.isThing())
            variable.asThing().isa().ifPresent(isaConstraint -> isaConstraint.removeHint(label));
        else throw GraknException.of(ILLEGAL_STATE);
    }

    private static Set<Label> getHintLabels(Variable variable) {
        if (variable.isType()) {
            if (variable.asType().sub().isPresent()) return variable.asType().sub().get().getTypeHints();
            return null;
        } else if (variable.isThing()) {
            if (variable.asThing().isa().isPresent()) return variable.asThing().isa().get().typeHints();
            return null;
        } else throw GraknException.of(ILLEGAL_STATE);
    }

    private static void addHintLabels(Variable variable, Set<Label> labels) {
        if (variable.isType()) variable.asType().sub().ifPresent(subConstraint -> subConstraint.addHints(labels));
        else if (variable.isThing())
            variable.asThing().isa().ifPresent(isaConstraint -> isaConstraint.addHints(labels));
        else throw GraknException.of(ILLEGAL_STATE);
    }

    private static void ensureHintsConformToTheirSuper(Variable variable, Set<Variable> visited, GraphManager graphManager) {
        if (variable == null || visited.contains(variable) || variable.reference().isLabel()) return;
        visited.add(variable);
        ensureHintsConformToTheirSuper(above(variable), visited, graphManager);
        removeHintsViolatingSuper(variable, graphManager);
    }

    private static void removeHintsViolatingSuper(Variable subVar, GraphManager graphManager) {
        TypeVariable supVar = above(subVar);
        if (supVar == null) return;
        if (supVar.reference().isLabel()) return;

        Set<Label> supLabels = getHintLabels(supVar);
        Set<Label> subLabels = getHintLabels(subVar);
        if (supLabels == null || supLabels.isEmpty()) return;

        if (subLabels == null) return;
        if (subLabels.isEmpty()) {
            Set<Label> subHintsOfSupLabels = supLabels.stream()
                    .map(label -> TypeImpl.of(graphManager, graphManager.schema().getType(label)))
                    .flatMap(TypeImpl::getSubtypes).map(TypeImpl::getLabel).map(Label::of).collect(Collectors.toSet());
            addHintLabels(subVar, subHintsOfSupLabels);
            return;
        }


        Set<Label> temp = new HashSet<>(subLabels);
        for (Label label : temp) {
            Type hintType = TypeImpl.of(graphManager, graphManager.schema().getType(label));
            //TODO use getProperLabel once that is available
            while (hintType != null && !supLabels.contains(Label.of(hintType.getLabel()))) {
                hintType = hintType.getSupertype();
            }
            if (hintType == null) removeHintLabel(subVar, label);
        }
    }

    private static Map<Label, TypeVariable> labelVarsFromConjunction(Conjunction conjunction) {
        Map<Label, TypeVariable> labels = new HashMap<>();

        conjunction.variables().stream().filter(Variable::isType).map(Variable::asType)
                .forEach(variable -> variable.label().ifPresent(labelConstraint ->
                                                                        labels.putIfAbsent(labelConstraint.properLabel(), variable)));

        return labels;
    }

    private static void addInferredSubLabels(TypeVariable variable, Set<Label> hints, Map<Label, TypeVariable> labelMap, GraphManager graphManager) {
        if (!variable.sub().isPresent()) {
            variable.sub(lowestCommonSuperType(hints, graphManager, labelMap), false);
        }
        variable.sub().get().addHints(hints);
    }

    private static void addInferredIsaLabels(ThingVariable variable, Set<Label> hints, Map<Label, TypeVariable> labelMap, GraphManager graphManager) {
        if (!variable.isa().isPresent()) {
            variable.isa(lowestCommonSuperType(hints, graphManager, labelMap), false);
        }
        variable.isa().get().addHints(hints);
    }

    private static TypeVariable lowestCommonSuperType(Set<Label> labels, GraphManager graphManager, Map<Label, TypeVariable> labelMap) {
        Set<Type> types = labels.stream().map(label ->
                                                      TypeImpl.of(graphManager, graphManager.schema().getType(label))).collect(Collectors.toSet());

        Type lowestCommonAncestor = null;
        for (Type type : types) {
            if (lowestCommonAncestor == null) {
                lowestCommonAncestor = type;
                continue;
            }
            Set<Type> superTypes = type.getSupertypes().collect(Collectors.toSet());
            while (!superTypes.contains(lowestCommonAncestor)) {
                lowestCommonAncestor = lowestCommonAncestor.getSupertype();
            }
        }

        assert lowestCommonAncestor != null;
        //TODO: use properLabel when avaialble
        return getOrCreateTypeVariable(Label.of(lowestCommonAncestor.getLabel()), labelMap);
    }

    private static TypeVariable getOrCreateTypeVariable(Label label, Map<Label, TypeVariable> labelMap) {
        if (!labelMap.containsKey(label)) {
            TypeVariable newTypeVar = new TypeVariable(Identifier.Variable.of(Reference.label(label.scopedName())));
            newTypeVar.label(label);
            labelMap.put(label, newTypeVar);
        }
        return labelMap.get(label);
    }

    private static void addInferredRoleLabels(ThingVariable variable, Map<Reference, Set<Label>> labels, InferenceVariables inferenceVariables) {
        Set<RelationConstraint> relationConstraints = variable.asThing().relation();
        for (RelationConstraint constraint : relationConstraints) {
            List<RelationConstraint.RolePlayer> rolePlayers = constraint.players();
            for (RelationConstraint.RolePlayer rolePlayer : rolePlayers) {
                TypeVariable typeVariable;
                if (rolePlayer.roleType().isPresent() && rolePlayer.roleType().get().reference().isName()) {
                    typeVariable = rolePlayer.roleType().get();
                } else {
                    typeVariable = inferenceVariables.getConversion(rolePlayer);
                }
                rolePlayer.addRoleTypeHints(labels.get(typeVariable.reference()));
            }
        }
    }


    private static Map<Reference, Set<Label>> computeHints(Set<Variable> inferenceVariables, GraphManager graphManager) {
        Conjunction inference = new Conjunction(inferenceVariables, Collections.emptySet());

        Map<Reference, Set<Label>> mapping = new HashMap<>();
        inference.traversal().execute(graphManager).forEachRemaining(
                result -> result.forEach((ref, vertex) -> {
                    mapping.putIfAbsent(ref, new HashSet<>());
                    mapping.get(ref).add(Label.of(vertex.asType().label(), vertex.asType().scope()));
                })
        );

        return mapping;
    }

    private static class ConstraintMapper {
        private final InferenceVariables inferenceVariables;
        private final HashMap<TypeVariable, Set<TypeVariable>> neighbours;
        private final TypeVariable metaThing;
        private final TypeVariable metaAttribute;
        private final TypeVariable metaRelation;
        private final TypeVariable metaRole;
        private final Conjunction conjunction;

        public ConstraintMapper(Conjunction conjunction) {
            this.inferenceVariables = new InferenceVariables();
            this.neighbours = new HashMap<>();
            this.conjunction = conjunction;
            this.metaThing = createMeta(Label.of(THING.toString()));
            this.metaAttribute = createMeta(Label.of(ATTRIBUTE.toString()));
            this.metaRelation = createMeta(Label.of(RELATION.toString()));
            this.metaRole = createMeta(Label.of(ROLE.toString(), RELATION.toString()));
            conjunction.variables().forEach(this::convertVariable);
            inferenceVariables.getInferenceVariables().forEach(this::putSubThingConstraintIfAbsent);
        }

        private TypeVariable createMeta(Label metaLabel) {

            Optional<TypeVariable> metaType = conjunction.variables().stream().filter(Variable::isType).map(Variable::asType)
                    .filter(variable -> variable.label().isPresent())
                    .filter(variable -> variable.label().get().properLabel().equals(metaLabel))
                    .findAny();

            if (metaType.isPresent()) return inferenceVariables.convert(metaType.get());
            TypeVariable newMetaType = new TypeVariable(Identifier.Variable.of(Reference.label(metaLabel.toString())));
            newMetaType.label(metaLabel);
            return newMetaType;
        }

        public InferenceVariables getInferenceVariables() {
            return inferenceVariables;
        }

        public HashMap<TypeVariable, Set<TypeVariable>> getVariableNeighbours() {
            return neighbours;
        }

        private void putSubThingConstraintIfAbsent(TypeVariable variable) {
            if (isMapped(variable)) variable.sub(metaThing, true);
        }

        private boolean isMapped(TypeVariable variable) {
            return !variable.reference().isLabel() && !variable.sub().isPresent() &&
                    !variable.label().isPresent() && variable.is().isEmpty();
        }

        private TypeVariable convertVariable(Variable variable) {
            if (inferenceVariables.hasConversion(variable)) return inferenceVariables.getConversion(variable);
            if (variable.isType()) {
                TypeVariable asTypeVar = inferenceVariables.convert(variable);
                neighbours.putIfAbsent(asTypeVar, new HashSet<>());
            } else convertThingVariable(variable.asThing());
            return inferenceVariables.getConversion(variable);
        }

        private void convertThingVariable(ThingVariable thingVariable) {
            TypeVariable inferenceVariable = inferenceVariables.convert(thingVariable);
            neighbours.putIfAbsent(inferenceVariable, new HashSet<>());

            if (thingVariable.constraints().isEmpty()) return;

            thingVariable.isa().ifPresent(constraint -> convertIsa(inferenceVariable, constraint));
            thingVariable.is().forEach(constraint -> convertIs(inferenceVariable, constraint));
            thingVariable.has().forEach(constraint -> convertHas(inferenceVariable, constraint));
            thingVariable.value().forEach(constraint -> convertValue(inferenceVariable, constraint));
            thingVariable.relation().forEach(constraint -> convertRelation(inferenceVariable, constraint));
        }

        private void convertRelation(TypeVariable owner, RelationConstraint relationConstraint) {
            if (isMapped(owner)) owner.sub(metaRelation, true);
            ThingVariable ownerThing = relationConstraint.owner();
            TypeVariable relationTypeVar;
            if (!ownerThing.isa().isPresent()) {
                relationTypeVar = inferenceVariables.newInferenceVariable();
                neighbours.put(relationTypeVar, new HashSet<>());
            } else {
                relationTypeVar = convertVariable(ownerThing.isa().get().type());
            }
            if (isMapped(relationTypeVar)) relationTypeVar.sub(metaRelation, true);
            for (RelationConstraint.RolePlayer rolePlayer : relationConstraint.players()) {
                TypeVariable playerType = convertVariable(rolePlayer.player());
                TypeVariable roleTypeVar = rolePlayer.roleType().orElse(null);
                if (roleTypeVar != null) {
                    roleTypeVar = convertVariable(roleTypeVar);
                    if (isMapped(roleTypeVar)) roleTypeVar.sub(metaRole, true);
                    addRelatesConstraint(owner, roleTypeVar);
                    addRelatesConstraint(relationTypeVar, roleTypeVar);
                }

                if (roleTypeVar == null || roleTypeVar.reference().isLabel()) {
                    TypeVariable rpInferenceVariable = inferenceVariables.convert(rolePlayer);
                    neighbours.put(rpInferenceVariable, new HashSet<>());
                    if (isMapped(rpInferenceVariable)) rpInferenceVariable.sub(metaRole, true);
                    addRelatesConstraint(owner, rpInferenceVariable);
                    addRelatesConstraint(relationTypeVar, rpInferenceVariable);
                    playerType.plays(null, rpInferenceVariable, null);
                    addNeighbour(playerType, rpInferenceVariable);
                } else {
                    playerType.plays(null, roleTypeVar, null);
                    addNeighbour(playerType, roleTypeVar);
                }
            }
        }

        private void addRelatesConstraint(TypeVariable owner, TypeVariable roleType) {
            if (owner != null && !owner.reference().isLabel()) owner.relates(roleType, null);
            if (owner != null) addNeighbour(owner, roleType);
        }

        private void convertHas(TypeVariable owner, HasConstraint hasConstraint) {
            TypeVariable attributeTypeVar = convertVariable(hasConstraint.attribute());
            owner.owns(attributeTypeVar, null, false);
            if (isMapped(attributeTypeVar)) attributeTypeVar.sub(metaAttribute, true);
            addNeighbour(owner, attributeTypeVar);
        }

        private void convertIsa(TypeVariable owner, IsaConstraint isaConstraint) {
            TypeVariable isaVar = convertVariable(isaConstraint.type());
            if (!isaConstraint.isExplicit()) owner.sub(isaConstraint.type(), false);
            else if (isaConstraint.type().reference().isName()) owner.is(isaConstraint.type());
            else if (isaConstraint.type().label().isPresent())
                owner.label(isaConstraint.type().label().get().properLabel());
            else GraknException.of(ILLEGAL_STATE);
            addNeighbour(isaVar, isaVar);
        }

        private void convertIs(TypeVariable owner, grakn.core.pattern.constraint.thing.IsConstraint isConstraint) {
            TypeVariable isVar = convertVariable(isConstraint.variable());
            owner.is(isVar);
            addNeighbour(owner, isVar);
        }

        private void convertValue(TypeVariable owner, ValueConstraint<?> constraint) {
            if (constraint.isBoolean()) owner.valueType(GraqlArg.ValueType.BOOLEAN);
            else if (constraint.isString()) owner.valueType(GraqlArg.ValueType.STRING);
            else if (constraint.isDateTime()) owner.valueType(GraqlArg.ValueType.DATETIME);
            else if (constraint.isDouble()) owner.valueType(GraqlArg.ValueType.DOUBLE);
            else if (constraint.isLong()) owner.valueType(GraqlArg.ValueType.LONG);
            else throw GraknException.of(ILLEGAL_STATE);
            if (isMapped(owner)) owner.sub(metaAttribute, true);
        }

        public void addNeighbour(TypeVariable from, TypeVariable to) {
            neighbours.get(from).add(to);
            neighbours.get(to).add(from);
        }

    }

    private static class InferenceVariables {

        private Map<Reference, TypeVariable> inferenceVars;
        private Map<RelationConstraint.RolePlayer, TypeVariable> rpInferenceVars;

        private Integer tempCounter;

        InferenceVariables() {
            this.tempCounter = 0;
            this.inferenceVars = new HashMap<>();
            this.rpInferenceVars = new HashMap<>();
        }

        TypeVariable newInferenceVariable() {
            TypeVariable tempVar = new TypeVariable(Identifier.Variable.of(
                    new grakn.core.pattern.variable.Reference.System("temp" + addAndGetCounter())));
            inferenceVars.put(tempVar.reference(), tempVar);
            return tempVar;
        }

        public TypeVariable convert(RelationConstraint.RolePlayer key) {
            if (!rpInferenceVars.containsKey(key)) {
                TypeVariable newTypeVar = new TypeVariable(Identifier.Variable.of(
                        new grakn.core.pattern.variable.Reference.System("temp" + addAndGetCounter())));
                rpInferenceVars.put(key, newTypeVar);
            }
            return rpInferenceVars.get(key);
        }

        public TypeVariable convert(Variable key) {
            if (!inferenceVars.containsKey(key.reference())) {
                TypeVariable newTypeVar = new TypeVariable(key.identifier());
                if (key.isType()) key.asType().constraints().forEach(newTypeVar::constrain);
                inferenceVars.put(key.reference(), newTypeVar);
            }
            return inferenceVars.get(key.reference());
        }

        public boolean hasConversion(Variable key) {
            return inferenceVars.containsKey(key.reference());
        }

        public Collection<TypeVariable> getInferenceVariables() {
            return inferenceVars.values();
        }

        private Integer addAndGetCounter() {
            return ++tempCounter;
        }

        public TypeVariable getConversion(Variable key) {
            return inferenceVars.get(key.reference());
        }

        public TypeVariable getConversion(RelationConstraint.RolePlayer rolePlayer) {
            return rpInferenceVars.get(rolePlayer);
        }
    }


}
