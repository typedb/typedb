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

import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Label;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.type.Type;
import grakn.core.graph.vertex.TypeVertex;
import grakn.core.logic.LogicCache;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.constraint.type.TypeConstraint;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static grakn.core.common.iterator.Iterators.iterate;
import static graql.lang.common.GraqlToken.Type.ATTRIBUTE;
import static graql.lang.common.GraqlToken.Type.RELATION;
import static graql.lang.common.GraqlToken.Type.ROLE;

public class TypeResolver {

    private final ConceptManager conceptMgr;
    private final TraversalEngine traversalEng;
    private final LogicCache logicCache;

    public TypeResolver(ConceptManager conceptMgr, TraversalEngine traversalEng, LogicCache logicCache) {
        this.conceptMgr = conceptMgr;
        this.traversalEng = traversalEng;
        this.logicCache = logicCache;
    }

    public Conjunction resolveVariablesExhaustive(Conjunction conjunction) {
        ConstraintMapper constraintMapper = new ConstraintMapper(conjunction);
        VariableHints variableHints = constraintMapper.getVariableHints();
        Map<Reference, Set<Label>> referenceHintsMapping =
                retrieveVariableHints(new HashSet<>(variableHints.getVariableHints()));
        long numOfTypes = traversalEng.graph().schema().stats().thingTypeCount();

        final Map<Identifier, TypeVariable> varHints = variableHints.varHints;


        for (Variable variable : conjunction.variables()) {
            if (variable.reference().isLabel()) continue;
            Set<Label> hintLabels = referenceHintsMapping.get(varHints.get(variable.identifier()).reference());
            if (variable.isThing()) {
                if (hintLabels.size() != numOfTypes) {
                    addInferredIsaLabels(variable.asThing(), referenceHintsMapping.get(varHints.get(variable.identifier()).reference()));
                }
            } else if (variable.isType() && hintLabels.size() != numOfTypes) {
                addInferredSubLabels(variable.asType(), referenceHintsMapping.get(varHints.get(variable.identifier()).reference()));
            }
        }
        return conjunction;
    }

    public Conjunction resolveVariables(Conjunction conjunction) {
        ConstraintMapper constraintMapper = new ConstraintMapper(conjunction);
        VariableHints variableHints = constraintMapper.getVariableHints();
        long numOfThings = traversalEng.graph().schema().stats().thingTypeCount();

        final Map<Identifier, TypeVariable> varHints = variableHints.varHints;

        for (Variable variable : conjunction.variables()) {
            if (variable.reference().isLabel()) continue;
            Set<Variable> neighbourhood = new HashSet<>();
            TypeVariable typeVariable = variableHints.getConversion(variable);
            neighbourhood.add(typeVariable);
            neighbourhood.addAll(constraintMapper.getVariableNeighbours().get(typeVariable));

            Map<Reference, Set<Label>> localTypeHints = retrieveVariableHints(neighbourhood);
            Set<Label> hintLabels = localTypeHints.get(varHints.get(variable.identifier()).reference());
            if (variable.isThing()) {
                if (hintLabels.size() != numOfThings) {
                    addInferredIsaLabels(variable.asThing(), hintLabels);
                }
            } else if (variable.isType() && hintLabels.size() != numOfThings) {
                addInferredSubLabels(variable.asType(), hintLabels);
            }
        }

        ensureHintsConformToTheirSuper(conjunction);
        return conjunction;
    }

    public Conjunction resolveLabels(Conjunction conjunction) {
        iterate(conjunction.variables()).filter(v -> v.isType() && v.asType().label().isPresent())
                .forEachRemaining(typeVar -> {
                    Label label = typeVar.asType().label().get().properLabel();
                    if (label.scope().isPresent()) {
                        Set<Label> labels = traversalEng.graph().schema().resolveRoleTypeLabels(label);
                        if (labels.isEmpty()) throw GraknException.of(TYPE_NOT_FOUND, label);
                        typeVar.addResolvedTypes(labels);
                    } else {
                        TypeVertex type = traversalEng.graph().schema().getType(label);
                        if (type == null) throw GraknException.of(TYPE_NOT_FOUND, label);
                        typeVar.addResolvedType(label);
                    }
                });
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

        Set<Label> supLabels = supVar.resolvedTypes();
        Set<Label> subLabels = subVar.resolvedTypes();
        if (supLabels.isEmpty()) return;

        if (subLabels.isEmpty()) {
            Set<Label> subHintsOfSupLabels = supLabels.stream().flatMap(label -> getType(label).getSubtypes())
                    .map(Type::getLabel).collect(Collectors.toSet());
            subVar.addResolvedTypes(subHintsOfSupLabels);
            return;
        }

        Set<Label> temp = new HashSet<>(subLabels);
        for (Label label : temp) {
            Type hintType = getType(label);
            while (hintType != null && !supLabels.contains(hintType.getLabel())) {
                hintType = hintType.getSupertype();
            }
            if (hintType == null) {
                subVar.removeResolvedType(label);
                if (subVar.resolvedTypes().isEmpty()) subVar.setSatisfiable(false);
            }
        }
    }

    private void addInferredSubLabels(TypeVariable variable, Set<Label> hints) {
        variable.addResolvedTypes(hints);
    }

    private void addInferredIsaLabels(ThingVariable variable, Set<Label> hints) {
        //TODO: use .getType(label) once ConceptManager can handle labels
        hints.removeIf(label -> traversalEng.graph().schema().getType(label).isAbstract());
        variable.addResolvedTypes(hints);
    }

    private Map<Reference, Set<Label>> retrieveVariableHints(Set<Variable> varHints) {
        Conjunction varHintsConjunction = new Conjunction(varHints, Collections.emptySet());
        varHintsConjunction = resolveLabels(varHintsConjunction);
        return logicCache.hinter().get(varHintsConjunction, conjunction -> {
            Map<Reference, Set<Label>> mapping = new HashMap<>();
            traversalEng.iterator(conjunction.traversal()).forEachRemaining(
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
            return conceptMgr.getThingType(label.name());
        }
    }

    private static class ConstraintMapper {
        private final VariableHints varHints;
        private final HashMap<TypeVariable, Set<TypeVariable>> neighbours;
        private final TypeVariable metaAttribute;
        private final TypeVariable metaRelation;
        private final TypeVariable metaRole;
        private final Conjunction conjunction;

        public ConstraintMapper(Conjunction conjunction) {
            this.varHints = new VariableHints();
            this.neighbours = new HashMap<>();
            this.conjunction = conjunction;
            this.metaAttribute = createMeta(Label.of(ATTRIBUTE.toString()));
            this.metaRelation = createMeta(Label.of(RELATION.toString()));
            this.metaRole = createMeta(Label.of(ROLE.toString(), RELATION.toString()));
            conjunction.variables().forEach(this::convertVariable);
        }

        private TypeVariable createMeta(Label metaLabel) {

            Optional<TypeVariable> metaType = conjunction.variables().stream().filter(Variable::isType).map(Variable::asType)
                    .filter(variable -> variable.label().isPresent())
                    .filter(variable -> variable.label().get().properLabel().equals(metaLabel))
                    .findAny();

            if (metaType.isPresent()) {
                TypeVariable newMetaType = varHints.convert(metaType.get());
                neighbours.putIfAbsent(newMetaType, new HashSet<>());
                return newMetaType;
            }

            TypeVariable newMetaType = new TypeVariable(Identifier.Variable.of(Reference.label(metaLabel.toString())));
            newMetaType.label(metaLabel);
            neighbours.putIfAbsent(newMetaType, new HashSet<>());
            return newMetaType;
        }

        public VariableHints getVariableHints() {
            return varHints;
        }

        public HashMap<TypeVariable, Set<TypeVariable>> getVariableNeighbours() {
            return neighbours;
        }

        private boolean isMapped(TypeVariable variable) {
            return !variable.reference().isLabel() && !variable.sub().isPresent() &&
                    !variable.label().isPresent() && variable.is().isEmpty();
        }

        private TypeVariable convertVariable(Variable variable) {
            if (varHints.hasConversion(variable)) return varHints.getConversion(variable);
            if (variable.isType()) {
                TypeVariable asTypeVar = varHints.convert(variable);
                addNeighboursOfTypeVariable(variable.asType(), asTypeVar);
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
            if (isMapped(owner)) addMetaType(owner, metaRelation);
            ThingVariable ownerThing = relationConstraint.owner();
            if (ownerThing.isa().isPresent()) {
                TypeVariable relationTypeVar = convertVariable(ownerThing.isa().get().type());
                if (isMapped(relationTypeVar)) addMetaType(relationTypeVar, metaRelation);
            }
            for (RelationConstraint.RolePlayer rolePlayer : relationConstraint.players()) {
                TypeVariable playerType = convertVariable(rolePlayer.player());
                TypeVariable roleTypeVar = rolePlayer.roleType().orElse(null);

                if (roleTypeVar != null) {
                    roleTypeVar = convertVariable(roleTypeVar);
                    if (isMapped(roleTypeVar)) addMetaType(roleTypeVar, metaRole);
                    addRelatesConstraint(owner, roleTypeVar);
                }

                if (roleTypeVar == null) {
                    TypeVariable rolePlayerHint = varHints.convert(rolePlayer);
                    neighbours.put(rolePlayerHint, new HashSet<>());
                    if (isMapped(rolePlayerHint)) addMetaType(rolePlayerHint, metaRole);
                    addRelatesConstraint(owner, rolePlayerHint);
                    playerType.plays(null, rolePlayerHint, null);
                    addNeighbours(playerType, rolePlayerHint);
                } else {
                    playerType.plays(null, roleTypeVar, null);
                    addNeighbours(playerType, roleTypeVar);
                }
            }
        }

        private void addRelatesConstraint(TypeVariable owner, TypeVariable roleType) {
            if (owner != null && !owner.reference().isLabel()) owner.relates(roleType, null);
            if (owner != null) addNeighbours(owner, roleType);
        }

        private void addMetaType(TypeVariable variable, TypeVariable meta) {
            TypeVariable metaConverted = varHints.convert(meta);
            variable.sub(metaConverted, false);
            addNeighbours(variable, metaConverted);
        }

        private void convertHas(TypeVariable owner, HasConstraint hasConstraint) {
            TypeVariable attributeTypeVar = convertVariable(hasConstraint.attribute());
            owner.owns(attributeTypeVar, null, false);
            if (isMapped(attributeTypeVar)) addMetaType(attributeTypeVar, metaAttribute);
            addNeighbours(owner, attributeTypeVar);
            assert attributeTypeVar.sub().isPresent();
            addNeighbours(owner, attributeTypeVar.sub().get().type());
        }

        private void convertIsa(TypeVariable owner, IsaConstraint isaConstraint) {
            TypeVariable isaVar = convertVariable(isaConstraint.type());
            if (!isaConstraint.isExplicit()) owner.sub(isaConstraint.type(), false);
            else if (isaConstraint.type().reference().isName()) owner.is(isaConstraint.type());
            else if (isaConstraint.type().label().isPresent())
                owner.label(isaConstraint.type().label().get().properLabel());
            else throw GraknException.of(ILLEGAL_STATE);
            addNeighbours(owner, isaVar);
        }

        private void convertIs(TypeVariable owner, grakn.core.pattern.constraint.thing.IsConstraint isConstraint) {
            TypeVariable isVar = convertVariable(isConstraint.variable());
            owner.is(isVar);
            addNeighbours(owner, isVar);
        }

        private void convertValue(TypeVariable owner, ValueConstraint<?> constraint) {
            if (constraint.isBoolean()) owner.valueType(GraqlArg.ValueType.BOOLEAN);
            else if (constraint.isString()) owner.valueType(GraqlArg.ValueType.STRING);
            else if (constraint.isDateTime()) owner.valueType(GraqlArg.ValueType.DATETIME);
            else if (constraint.isDouble()) owner.valueType(GraqlArg.ValueType.DOUBLE);
            else if (constraint.isLong()) owner.valueType(GraqlArg.ValueType.LONG);
            else throw GraknException.of(ILLEGAL_STATE);
            if (isMapped(owner)) addMetaType(owner, metaAttribute);
        }

        public void addNeighbours(TypeVariable from, TypeVariable to) {
            neighbours.putIfAbsent(from, new HashSet<>());
            neighbours.putIfAbsent(to, new HashSet<>());
            neighbours.get(from).add(to);
            neighbours.get(to).add(from);
        }

        //TODO: rename
        public void addNeighboursOfTypeVariable(TypeVariable fromCopy, TypeVariable toCopy) {
            for (TypeConstraint constraint : fromCopy.constraints()) {
                if (constraint.isSub()) {
                    addNeighbours(fromCopy, constraint.asSub().type());
                } else if (constraint.isOwns()) {
                    addNeighbours(fromCopy, constraint.asOwns().attribute());
                } else if (constraint.isPlays()) {
                    addNeighbours(fromCopy, constraint.asPlays().role());
                } else if (constraint.isRelates()) {
                    addNeighbours(fromCopy, constraint.asRelates().role());
                } else if (constraint.isIs()) {
                    addNeighbours(fromCopy, constraint.asIs().variable());
                }
            }
        }

    }

    private static class VariableHints {

        private final Map<Identifier, TypeVariable> varHints;
        private final Map<RelationConstraint.RolePlayer, TypeVariable> rolePlayerHints;

        private Integer tempCounter;

        VariableHints() {
            this.tempCounter = 0;
            this.varHints = new HashMap<>();
            this.rolePlayerHints = new HashMap<>();
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
            if (!varHints.containsKey(key.identifier())) {
                TypeVariable newTypeVar;
                if (key.reference().isAnonymous()) {
                    newTypeVar = new TypeVariable(Identifier.Variable.of(
                            new SystemReference("temp" + addAndGetCounter())));
                } else newTypeVar = new TypeVariable(key.identifier());
                if (key.isType()) {
                    newTypeVar.copyConstraints(key.asType());
                }
                varHints.put(key.identifier(), newTypeVar);
            }
            return varHints.get(key.identifier());
        }

        public boolean hasConversion(Variable key) {
            return varHints.containsKey(key.identifier());
        }

        public Collection<TypeVariable> getVariableHints() {
            return varHints.values();
        }

        private Integer addAndGetCounter() {
            return ++tempCounter;
        }

        public TypeVariable getConversion(Variable key) {
            return varHints.get(key.identifier());
        }
    }

}
