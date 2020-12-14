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

package grakn.core.logic.tool;

import grakn.core.common.concurrent.ExecutorService;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Label;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.type.Type;
import grakn.core.logic.LogicCache;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.variable.SystemReference;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.Variable;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
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
import static grakn.core.common.producer.Producers.buffer;
import static graql.lang.common.GraqlToken.Type.ATTRIBUTE;
import static graql.lang.common.GraqlToken.Type.RELATION;
import static graql.lang.common.GraqlToken.Type.ROLE;
import static graql.lang.common.GraqlToken.Type.THING;

public class TypeHinter {

    private final ConceptManager conceptMgr;
    private final TraversalEngine traversalEng;
    private final LogicCache logicCache;

    public TypeHinter(ConceptManager conceptMgr, TraversalEngine traversalEng, LogicCache logicCache) {
        this.conceptMgr = conceptMgr;
        this.traversalEng = traversalEng;
        this.logicCache = logicCache;
    }

    public Conjunction computeHintsExhaustive(Conjunction conjunction) {
        return computeHintsExhaustive(conjunction, ExecutorService.PARALLELISATION_FACTOR);
    }

    public Conjunction computeHintsExhaustive(Conjunction conjunction, int parallelisation) {
        ConstraintMapper constraintMapper = new ConstraintMapper(conjunction);
        VariableHints variableHints = constraintMapper.getVariableHints();
        Map<Label, TypeVariable> labelMap = labelVarsFromConjunction(conjunction);
        Map<Reference, Set<Label>> referenceHintsMapping =
                retrieveVariableHints(new HashSet<>(variableHints.getVariableHints()), parallelisation);
        long numOfThings = traversalEng.graph().schema().stats().thingTypeCount();

        for (Variable variable : conjunction.variables()) {
            if (variable.reference().isLabel()) continue;
            Set<Label> hintLabels = referenceHintsMapping.get(variable.reference());
            if (variable.isThing()) {
                if (hintLabels.size() != numOfThings) {
                    addInferredIsaLabels(variable.asThing(), referenceHintsMapping.get(variable.reference()), labelMap);
                }
                addInferredRoleLabels(variable.asThing(), referenceHintsMapping, variableHints);
            } else if (variable.isType() && hintLabels.size() != numOfThings) {
                addInferredSubLabels(variable.asType(), referenceHintsMapping.get(variable.reference()), labelMap);
            }
        }
        return conjunction;
    }

    public Conjunction computeHints(Conjunction conjunction) {
        return computeHints(conjunction, ExecutorService.PARALLELISATION_FACTOR);
    }

    public Conjunction computeHints(Conjunction conjunction, int parallelisation) {
        ConstraintMapper constraintMapper = new ConstraintMapper(conjunction);
        VariableHints variableHints = constraintMapper.getVariableHints();
        Map<Label, TypeVariable> labelMap = labelVarsFromConjunction(conjunction);
        long numOfThings = traversalEng.graph().schema().stats().thingTypeCount();

        for (Variable variable : conjunction.variables()) {
            if (variable.reference().isLabel()) continue;
            Set<Variable> neighbourhood = new HashSet<>();
            TypeVariable typeVariable = variableHints.getConversion(variable);
            neighbourhood.add(typeVariable);
            neighbourhood.addAll(constraintMapper.getVariableNeighbours().get(typeVariable));
            Map<Reference, Set<Label>> localTypeHints = retrieveVariableHints(neighbourhood, parallelisation);
            Set<Label> hintLabels = localTypeHints.get(variable.reference());
            if (variable.isThing()) {
                if (hintLabels.size() != numOfThings) {
                    addInferredIsaLabels(variable.asThing(), hintLabels, labelMap);
                }
                addInferredRoleLabels(variable.asThing(), localTypeHints, variableHints);
            } else if (variable.isType() && hintLabels.size() != numOfThings) {
                addInferredSubLabels(variable.asType(), localTypeHints.get(variable.reference()), labelMap);
            }
        }

        ensureHintsConformToTheirSuper(conjunction);
        return conjunction;
    }

    private void ensureHintsConformToTheirSuper(Conjunction conjunction) {
        Set<Variable> visited = new HashSet<>();
        conjunction.variables().forEach(variable -> ensureHintsConformToTheirSuper(variable, visited));
    }

    private TypeVariable above(Variable variable) {
        if (variable.isType()) {
            if (variable.asType().sub().isPresent()) return variable.asType().sub().get().type();
            return null;
        } else if (variable.isThing()) {
            if (variable.asThing().isa().isPresent()) return variable.asThing().isa().get().type();
            return null;
        } else throw GraknException.of(ILLEGAL_STATE);
    }

    private void removeHintLabel(Variable variable, Label label) {
        if (variable.isType()) variable.asType().sub().ifPresent(subConstraint -> subConstraint.removeHint(label));
        else if (variable.isThing())
            variable.asThing().isa().ifPresent(isaConstraint -> isaConstraint.removeHint(label));
        else throw GraknException.of(ILLEGAL_STATE);
    }

    private Set<Label> getHintLabels(Variable variable) {
        if (variable.isType()) {
            if (variable.asType().sub().isPresent()) return variable.asType().sub().get().getTypeHints();
            return null;
        } else if (variable.isThing()) {
            if (variable.asThing().isa().isPresent()) return variable.asThing().isa().get().getTypeHints();
            return null;
        } else throw GraknException.of(ILLEGAL_STATE);
    }

    private void addHintLabels(Variable variable, Set<Label> labels) {
        if (variable.isType()) variable.asType().sub().ifPresent(subConstraint -> subConstraint.addHints(labels));
        else if (variable.isThing())
            variable.asThing().isa().ifPresent(isaConstraint -> isaConstraint.addHints(labels));
        else throw GraknException.of(ILLEGAL_STATE);
    }

    private void ensureHintsConformToTheirSuper(Variable variable, Set<Variable> visited) {
        if (variable == null || visited.contains(variable) || variable.reference().isLabel()) return;
        visited.add(variable);
        ensureHintsConformToTheirSuper(above(variable), visited);
        removeHintsViolatingSuper(variable);
    }

    private void removeHintsViolatingSuper(Variable subVar) {
        TypeVariable supVar = above(subVar);
        if (supVar == null) return;
        if (supVar.reference().isLabel()) return;

        Set<Label> supLabels = getHintLabels(supVar);
        Set<Label> subLabels = getHintLabels(subVar);
        if (supLabels == null || supLabels.isEmpty()) return;

        if (subLabels == null) return;
        if (subLabels.isEmpty()) {
            Set<Label> subHintsOfSupLabels = supLabels.stream().flatMap(label -> getType(label).getSubtypes())
                    .map(Type::getLabel).collect(Collectors.toSet());
            addHintLabels(subVar, subHintsOfSupLabels);
            return;
        }

        Set<Label> temp = new HashSet<>(subLabels);
        for (Label label : temp) {
            Type hintType = getType(label);
            while (hintType != null && !supLabels.contains(hintType.getLabel())) {
                hintType = hintType.getSupertype();
            }
            if (hintType == null) removeHintLabel(subVar, label);
        }
    }

    private Map<Label, TypeVariable> labelVarsFromConjunction(Conjunction conjunction) {
        Map<Label, TypeVariable> labels = new HashMap<>();
        conjunction.variables().stream().filter(Variable::isType).map(Variable::asType)
                .forEach(variable -> variable.label().ifPresent(labelConstraint -> labels.putIfAbsent(labelConstraint.properLabel(), variable)));
        return labels;
    }

    private void addInferredSubLabels(TypeVariable variable, Set<Label> hints, Map<Label, TypeVariable> labelMap) {
        if (!variable.sub().isPresent()) {
            variable.sub(lowestCommonSuperType(hints, labelMap), false);
        }
        variable.sub().get().addHints(hints);
    }

    private void addInferredIsaLabels(ThingVariable variable, Set<Label> hints, Map<Label, TypeVariable> labelMap) {
        //TODO: use .getType(label) once ConceptManager can handle labels
        hints.removeIf(label -> conceptMgr.getType(label.scopedName()).isAbstract());
        if (!variable.isa().isPresent()) {
            variable.isa(lowestCommonSuperType(hints, labelMap), false);
        }
        variable.isa().get().addHints(hints);
    }

    private TypeVariable lowestCommonSuperType(Set<Label> labels, Map<Label, TypeVariable> labelMap) {
        Set<Type> types = labels.stream().map(this::getType).collect(Collectors.toSet());

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
        return getOrCreateTypeVariable(lowestCommonAncestor.getLabel(), labelMap);
    }

    private TypeVariable getOrCreateTypeVariable(Label label, Map<Label, TypeVariable> labelMap) {
        if (!labelMap.containsKey(label)) {
            TypeVariable newTypeVar = new TypeVariable(Identifier.Variable.of(Reference.label(label.scopedName())));
            newTypeVar.label(label);
            labelMap.put(label, newTypeVar);
        }
        return labelMap.get(label);
    }

    private void addInferredRoleLabels(ThingVariable variable, Map<Reference, Set<Label>> labels, VariableHints varHints) {
        Set<RelationConstraint> relationConstraints = variable.asThing().relation();
        for (RelationConstraint constraint : relationConstraints) {
            List<RelationConstraint.RolePlayer> rolePlayers = constraint.players();
            for (RelationConstraint.RolePlayer rolePlayer : rolePlayers) {
                TypeVariable typeVariable;
                if (rolePlayer.roleType().isPresent() && rolePlayer.roleType().get().reference().isName()) {
                    typeVariable = rolePlayer.roleType().get();
                } else {
                    typeVariable = varHints.getConversion(rolePlayer);
                }
                rolePlayer.addRoleTypeHints(labels.get(typeVariable.reference()));
            }
        }
    }

    private Map<Reference, Set<Label>> retrieveVariableHints(Set<Variable> varHints, int parallelisation) {
        Conjunction varHintsConjunction = new Conjunction(varHints, Collections.emptySet());
        return logicCache.hinter().get(varHintsConjunction, conjunction -> {
            Map<Reference, Set<Label>> mapping = new HashMap<>();
            buffer(traversalEng.producer(conjunction.traversal(), parallelisation)).iterator().forEachRemaining(
                    result -> result.forEach((ref, vertex) -> {
                        mapping.putIfAbsent(ref, new HashSet<>());
                        mapping.get(ref).add(Label.of(vertex.asType().label(), vertex.asType().scope()));
                    })
            );
            return mapping;
        });
    }

    private Type getType(Label label) {
        if (label.scope().isPresent()) {
            assert conceptMgr.getRelationType(label.scope().get()) != null;
            return conceptMgr.getRelationType(label.scope().get()).getRelates(label.name());
        } else {
            return conceptMgr.getType(label.name());
        }
    }

    private static class ConstraintMapper {
        private final VariableHints varHints;
        private final HashMap<TypeVariable, Set<TypeVariable>> neighbours;
        private final TypeVariable metaThing;
        private final TypeVariable metaAttribute;
        private final TypeVariable metaRelation;
        private final TypeVariable metaRole;
        private final Conjunction conjunction;

        public ConstraintMapper(Conjunction conjunction) {
            this.varHints = new VariableHints();
            this.neighbours = new HashMap<>();
            this.conjunction = conjunction;
            this.metaThing = createMeta(Label.of(THING.toString()));
            this.metaAttribute = createMeta(Label.of(ATTRIBUTE.toString()));
            this.metaRelation = createMeta(Label.of(RELATION.toString()));
            this.metaRole = createMeta(Label.of(ROLE.toString(), RELATION.toString()));
            conjunction.variables().forEach(this::convertVariable);
            varHints.getVariableHints().forEach(this::putSubThingConstraintIfAbsent);
        }

        private TypeVariable createMeta(Label metaLabel) {

            Optional<TypeVariable> metaType = conjunction.variables().stream().filter(Variable::isType).map(Variable::asType)
                    .filter(variable -> variable.label().isPresent())
                    .filter(variable -> variable.label().get().properLabel().equals(metaLabel))
                    .findAny();

            if (metaType.isPresent()) return varHints.convert(metaType.get());
            TypeVariable newMetaType = new TypeVariable(Identifier.Variable.of(Reference.label(metaLabel.toString())));
            newMetaType.label(metaLabel);
            return newMetaType;
        }

        public VariableHints getVariableHints() {
            return varHints;
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
            if (varHints.hasConversion(variable)) return varHints.getConversion(variable);
            if (variable.isType()) {
                TypeVariable asTypeVar = varHints.convert(variable);
                neighbours.putIfAbsent(asTypeVar, new HashSet<>());
            } else convertThingVariable(variable.asThing());
            return varHints.getConversion(variable);
        }

        private void convertThingVariable(ThingVariable thingVariable) {
            TypeVariable varHint = varHints.convert(thingVariable);
            neighbours.putIfAbsent(varHint, new HashSet<>());

            if (thingVariable.constraints().isEmpty()) return;

            thingVariable.isa().ifPresent(constraint -> convertIsa(varHint, constraint));
            thingVariable.is().forEach(constraint -> convertIs(varHint, constraint));
            thingVariable.has().forEach(constraint -> convertHas(varHint, constraint));
            thingVariable.value().forEach(constraint -> convertValue(varHint, constraint));
            thingVariable.relation().forEach(constraint -> convertRelation(varHint, constraint));
        }

        private void convertRelation(TypeVariable owner, RelationConstraint relationConstraint) {
            if (isMapped(owner)) owner.sub(metaRelation, true);
            ThingVariable ownerThing = relationConstraint.owner();
            TypeVariable relationTypeVar;
            if (!ownerThing.isa().isPresent()) {
                relationTypeVar = varHints.newHintingeVariable();
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
                    TypeVariable rolePlayerHint = varHints.convert(rolePlayer);
                    neighbours.put(rolePlayerHint, new HashSet<>());
                    if (isMapped(rolePlayerHint)) rolePlayerHint.sub(metaRole, true);
                    addRelatesConstraint(owner, rolePlayerHint);
                    addRelatesConstraint(relationTypeVar, rolePlayerHint);
                    playerType.plays(null, rolePlayerHint, null);
                    addNeighbour(playerType, rolePlayerHint);
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
            else throw GraknException.of(ILLEGAL_STATE);
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

    private static class VariableHints {

        private final Map<Reference, TypeVariable> varHints;
        private final Map<RelationConstraint.RolePlayer, TypeVariable> rolePlayerHints;

        private Integer tempCounter;

        VariableHints() {
            this.tempCounter = 0;
            this.varHints = new HashMap<>();
            this.rolePlayerHints = new HashMap<>();
        }

        TypeVariable newHintingeVariable() {
            TypeVariable tempVar = new TypeVariable(Identifier.Variable.of(
                    new SystemReference("temp" + addAndGetCounter())));
            varHints.put(tempVar.reference(), tempVar);
            return tempVar;
        }

        public TypeVariable convert(RelationConstraint.RolePlayer key) {
            if (!rolePlayerHints.containsKey(key)) {
                TypeVariable newTypeVar = new TypeVariable(Identifier.Variable.of(
                        new SystemReference("temp" + addAndGetCounter())));
                rolePlayerHints.put(key, newTypeVar);
            }
            return rolePlayerHints.get(key);
        }

        public TypeVariable convert(Variable key) {
            if (!varHints.containsKey(key.reference())) {
                TypeVariable newTypeVar = new TypeVariable(key.identifier());
                if (key.isType()) key.asType().constraints().forEach(newTypeVar::constrain);
                varHints.put(key.reference(), newTypeVar);
            }
            return varHints.get(key.reference());
        }

        public boolean hasConversion(Variable key) {
            return varHints.containsKey(key.reference());
        }

        public Collection<TypeVariable> getVariableHints() {
            return varHints.values();
        }

        private Integer addAndGetCounter() {
            return ++tempCounter;
        }

        public TypeVariable getConversion(Variable key) {
            return varHints.get(key.reference());
        }

        public TypeVariable getConversion(RelationConstraint.RolePlayer rolePlayer) {
            return rolePlayerHints.get(rolePlayer);
        }
    }

}
