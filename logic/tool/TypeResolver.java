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
        ResolverVariables resolverVariables = constraintMapper.getResolveVars();
        Map<Reference, Set<Label>> referenceResolversMapping =
                retrieveResolveTypes(new HashSet<>(resolverVariables.getResolveVars()));
        long numOfTypes = traversalEng.graph().schema().stats().thingTypeCount();

        final Map<Identifier, TypeVariable> resolveeVars = resolverVariables.resolveeVars;


        for (Variable variable : conjunction.variables()) {
            if (variable.reference().isLabel()) continue;
            Set<Label> resolveLabels = referenceResolversMapping.get(resolveeVars.get(variable.identifier()).reference());
            if (variable.isThing()) {
                if (resolveLabels.size() != numOfTypes) {
                    addInferredIsaLabels(variable.asThing(), referenceResolversMapping.get(resolveeVars.get(variable.identifier()).reference()));
                }
            } else if (variable.isType() && resolveLabels.size() != numOfTypes) {
                addInferredSubLabels(variable.asType(), referenceResolversMapping.get(resolveeVars.get(variable.identifier()).reference()));
            }
        }
        return conjunction;
    }

    public Conjunction resolveVariables(Conjunction conjunction) {
        ConstraintMapper constraintMapper = new ConstraintMapper(conjunction);
        ResolverVariables resolverVariables = constraintMapper.getResolveVars();
        long numOfThings = traversalEng.graph().schema().stats().thingTypeCount();

        final Map<Identifier, TypeVariable> resolveLabels = resolverVariables.resolveeVars;

        for (Variable variable : conjunction.variables()) {
            if (variable.reference().isLabel()) continue;
            Set<Variable> neighbourhood = new HashSet<>();
            TypeVariable typeVariable = resolverVariables.getConversion(variable);
            neighbourhood.add(typeVariable);
            neighbourhood.addAll(constraintMapper.getVariableNeighbours().get(typeVariable));

            Map<Reference, Set<Label>> localResolveType = retrieveResolveTypes(neighbourhood);
            Set<Label> resolveTypes = localResolveType.get(resolveLabels.get(variable.identifier()).reference());
            if (variable.isThing()) {
                if (resolveTypes.size() != numOfThings) {
                    addInferredIsaLabels(variable.asThing(), resolveTypes);
                }
            } else if (variable.isType() && resolveTypes.size() != numOfThings) {
                addInferredSubLabels(variable.asType(), resolveTypes);
            }
        }

        ensureResolvedConformToTheirSuper(conjunction);
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

    private void ensureResolvedConformToTheirSuper(Conjunction conjunction) {
        Set<Variable> visited = new HashSet<>();
        conjunction.variables().forEach(variable -> ensureResolvedConformToTheirSuper(variable, visited));
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

    private void ensureResolvedConformToTheirSuper(Variable variable, Set<Variable> visited) {
        if (variable == null || visited.contains(variable) || variable.reference().isLabel()) return;
        visited.add(variable);
        ensureResolvedConformToTheirSuper(above(variable), visited);
        removeVarsViolatingSuper(variable);
    }

    private void removeVarsViolatingSuper(Variable subVar) {
        TypeVariable supVar = above(subVar);
        if (supVar == null) return;
        if (supVar.reference().isLabel()) return;

        Set<Label> supLabels = supVar.resolvedTypes();
        Set<Label> subLabels = subVar.resolvedTypes();
        if (supLabels.isEmpty()) return;

        if (subLabels.isEmpty()) {
            Set<Label> subResolvedOfSupLabels = supLabels.stream().flatMap(label -> getType(label).getSubtypes())
                    .map(Type::getLabel).collect(Collectors.toSet());
            subVar.addResolvedTypes(subResolvedOfSupLabels);
            return;
        }

        Set<Label> temp = new HashSet<>(subLabels);
        for (Label label : temp) {
            Type ResolvedType = getType(label);
            while (ResolvedType != null && !supLabels.contains(ResolvedType.getLabel())) {
                ResolvedType = ResolvedType.getSupertype();
            }
            if (ResolvedType == null) {
                subVar.removeResolvedType(label);
                if (subVar.resolvedTypes().isEmpty()) subVar.setSatisfiable(false);
            }
        }
    }

    private void addInferredSubLabels(TypeVariable variable, Set<Label> resolvedLabels) {
        variable.addResolvedTypes(resolvedLabels);
    }

    private void addInferredIsaLabels(ThingVariable variable, Set<Label> resolveLabels) {
        //TODO: use .getType(label) once ConceptManager can handle labels
        resolveLabels.removeIf(label -> traversalEng.graph().schema().getType(label).isAbstract());
        variable.addResolvedTypes(resolveLabels);
    }

    private Map<Reference, Set<Label>> retrieveResolveTypes(Set<Variable> resolveVars) {
        Conjunction resolveVariableConjunction = new Conjunction(resolveVars, Collections.emptySet());
        resolveVariableConjunction = resolveLabels(resolveVariableConjunction);
        return logicCache.resolver().get(resolveVariableConjunction, conjunction -> {
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
        private final ResolverVariables resolverVariables;
        private final HashMap<TypeVariable, Set<TypeVariable>> neighbours;
        private final TypeVariable rootAttributeType;
        private final TypeVariable rootRelationType;
        private final TypeVariable rootRoleType;
        private final Conjunction conjunction;

        public ConstraintMapper(Conjunction conjunction) {
            this.resolverVariables = new ResolverVariables();
            this.neighbours = new HashMap<>();
            this.conjunction = conjunction;
            this.rootAttributeType = createRootTypeVar(Label.of(ATTRIBUTE.toString()));
            this.rootRelationType = createRootTypeVar(Label.of(RELATION.toString()));
            this.rootRoleType = createRootTypeVar(Label.of(ROLE.toString(), RELATION.toString()));
            conjunction.variables().forEach(this::convertVariable);
        }

        private TypeVariable createRootTypeVar(Label rootLabel) {

            Optional<TypeVariable> rootType = conjunction.variables().stream().filter(Variable::isType).map(Variable::asType)
                    .filter(variable -> variable.label().isPresent())
                    .filter(variable -> variable.label().get().properLabel().equals(rootLabel))
                    .findAny();

            if (rootType.isPresent()) {
                TypeVariable convertedRootType = resolverVariables.convert(rootType.get());
                neighbours.putIfAbsent(convertedRootType, new HashSet<>());
                return convertedRootType;
            }

            TypeVariable newRootType = new TypeVariable(Identifier.Variable.of(Reference.label(rootLabel.toString())));
            newRootType.label(rootLabel);
            neighbours.putIfAbsent(newRootType, new HashSet<>());
            return newRootType;
        }

        public ResolverVariables getResolveVars() {
            return resolverVariables;
        }

        public HashMap<TypeVariable, Set<TypeVariable>> getVariableNeighbours() {
            return neighbours;
        }

        private boolean noInformation(TypeVariable variable) {
            return !variable.reference().isLabel() && !variable.sub().isPresent() &&
                    !variable.label().isPresent() && variable.is().isEmpty();
        }

        private TypeVariable convertVariable(Variable variable) {
            if (resolverVariables.hasConversion(variable)) return resolverVariables.getConversion(variable);
            if (variable.isType()) {
                TypeVariable asTypeVar = resolverVariables.convert(variable);
                addNeighboursOfTypeVariable(variable.asType(), asTypeVar);
                neighbours.putIfAbsent(asTypeVar, new HashSet<>());
            } else convertThingVariable(variable.asThing());
            return resolverVariables.getConversion(variable);
        }

        private void convertThingVariable(ThingVariable thingVariable) {
            TypeVariable resolveVar = resolverVariables.convert(thingVariable);
            neighbours.putIfAbsent(resolveVar, new HashSet<>());

            if (thingVariable.constraints().isEmpty()) return;

            thingVariable.isa().ifPresent(constraint -> convertIsa(resolveVar, constraint));
            thingVariable.is().forEach(constraint -> convertIs(resolveVar, constraint));
            thingVariable.has().forEach(constraint -> convertHas(resolveVar, constraint));
            thingVariable.value().forEach(constraint -> convertValue(resolveVar, constraint));
            thingVariable.relation().forEach(constraint -> convertRelation(resolveVar, constraint));

        }

        private void convertRelation(TypeVariable owner, RelationConstraint relationConstraint) {
            if (noInformation(owner)) addRootTypeVar(owner, rootRelationType);
            ThingVariable ownerThing = relationConstraint.owner();
            if (ownerThing.isa().isPresent()) {
                TypeVariable relationTypeVar = convertVariable(ownerThing.isa().get().type());
                if (noInformation(relationTypeVar)) addRootTypeVar(relationTypeVar, rootRelationType);
            }
            for (RelationConstraint.RolePlayer rolePlayer : relationConstraint.players()) {
                TypeVariable playerType = convertVariable(rolePlayer.player());
                TypeVariable roleTypeVar = rolePlayer.roleType().orElse(null);

                if (roleTypeVar != null) {
                    roleTypeVar = convertVariable(roleTypeVar);
                    if (noInformation(roleTypeVar)) addRootTypeVar(roleTypeVar, rootRoleType);
                    addRelatesConstraint(owner, roleTypeVar);
                }

                if (roleTypeVar == null) {
                    TypeVariable resolveRolePlayer = resolverVariables.convert(rolePlayer);
                    neighbours.put(resolveRolePlayer, new HashSet<>());
                    if (noInformation(resolveRolePlayer)) addRootTypeVar(resolveRolePlayer, rootRoleType);
                    addRelatesConstraint(owner, resolveRolePlayer);
                    playerType.plays(null, resolveRolePlayer, null);
                    addNeighbours(playerType, resolveRolePlayer);
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

        private void addRootTypeVar(TypeVariable variable, TypeVariable rootTypeVar) {
            TypeVariable rootConverted = resolverVariables.convert(rootTypeVar);
            variable.sub(rootConverted, false);
            addNeighbours(variable, rootConverted);
        }

        private void convertHas(TypeVariable owner, HasConstraint hasConstraint) {
            TypeVariable attributeTypeVar = convertVariable(hasConstraint.attribute());
            owner.owns(attributeTypeVar, null, false);
            if (noInformation(attributeTypeVar)) addRootTypeVar(attributeTypeVar, rootAttributeType);
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
            else if (constraint.isVariable()) resolverVariables.convert(constraint.asVariable().value());
            else throw GraknException.of(ILLEGAL_STATE);
            if (noInformation(owner)) addRootTypeVar(owner, rootAttributeType);
        }

        public void addNeighbours(TypeVariable from, TypeVariable to) {
            neighbours.putIfAbsent(from, new HashSet<>());
            neighbours.putIfAbsent(to, new HashSet<>());
            neighbours.get(from).add(to);
            neighbours.get(to).add(from);
        }

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

    private static class ResolverVariables {

        private final Map<Identifier, TypeVariable> resolveeVars;
        private final Map<RelationConstraint.RolePlayer, TypeVariable> rolePlayerResolveVars;

        private Integer tempCounter;

        ResolverVariables() {
            this.tempCounter = 0;
            this.resolveeVars = new HashMap<>();
            this.rolePlayerResolveVars = new HashMap<>();
        }

        public TypeVariable convert(RelationConstraint.RolePlayer key) {
            if (!rolePlayerResolveVars.containsKey(key)) {
                TypeVariable newTypeVar = new TypeVariable(Identifier.Variable.of(
                        new SystemReference("temp" + addAndGetCounter())));
                rolePlayerResolveVars.put(key, newTypeVar);
            }
            return rolePlayerResolveVars.get(key);
        }

        public TypeVariable convert(Variable key) {
            if (!resolveeVars.containsKey(key.identifier())) {
                TypeVariable newTypeVar;
                if (key.reference().isAnonymous()) {
                    newTypeVar = new TypeVariable(Identifier.Variable.of(
                            new SystemReference("temp" + addAndGetCounter())));
                } else newTypeVar = new TypeVariable(key.identifier());
                if (key.isType()) {
                    newTypeVar.copyConstraints(key.asType());
                }
                resolveeVars.put(key.identifier(), newTypeVar);
            }
            return resolveeVars.get(key.identifier());
        }

        public boolean hasConversion(Variable key) {
            return resolveeVars.containsKey(key.identifier());
        }

        public Collection<TypeVariable> getResolveVars() {
            return resolveeVars.values();
        }

        private Integer addAndGetCounter() {
            return ++tempCounter;
        }

        public TypeVariable getConversion(Variable key) {
            return resolveeVars.get(key.identifier());
        }
    }

}
