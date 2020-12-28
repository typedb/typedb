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
import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_RESOLVABLE;
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
        Resolvers resolverVariables = constraintMapper.resolvers();
        Map<Reference, Set<Label>> referenceResolversMapping =
                retrieveResolveTypes(new HashSet<>(resolverVariables.resolvers()));
        long numOfTypes = traversalEng.graph().schema().stats().thingTypeCount();

        final Map<Identifier, TypeVariable> resolveeVars = resolverVariables.typeResolvers;

        for (Variable variable : conjunction.variables()) {
            if (variable.reference().isLabel()) continue;
            Set<Label> resolveLabels = referenceResolversMapping.get(resolveeVars.get(variable.identifier()).reference());
            if (resolveLabels == null) {
                throw GraknException.of(TYPE_NOT_RESOLVABLE, variable.toString());
            }

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
        Resolvers resolverVariables = constraintMapper.resolvers();
        long numOfThings = traversalEng.graph().schema().stats().thingTypeCount();

        final Map<Identifier, TypeVariable> resolveLabels = resolverVariables.typeResolvers;

        for (Variable variable : conjunction.variables()) {
            if (variable.reference().isLabel()) continue;
            Set<Variable> neighbourhood = new HashSet<>();
            TypeVariable typeVariable = resolverVariables.resolver(variable);
            neighbourhood.add(typeVariable);
            neighbourhood.addAll(constraintMapper.neighbours().get(typeVariable));

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
        private final Resolvers resolvers;
        private final HashMap<TypeVariable, Set<TypeVariable>> neighbours;
        private final TypeVariable rootAttributeType;
        private final TypeVariable rootRelationType;
        private final TypeVariable rootRoleType;
        private final Conjunction conjunction;

        ConstraintMapper(Conjunction conjunction) {
            this.resolvers = new Resolvers();
            this.neighbours = new HashMap<>();
            this.conjunction = conjunction;
            this.rootAttributeType = createRootTypeVar(Label.of(ATTRIBUTE.toString()));
            this.rootRelationType = createRootTypeVar(Label.of(RELATION.toString()));
            this.rootRoleType = createRootTypeVar(Label.of(ROLE.toString(), RELATION.toString()));
            conjunction.variables().forEach(this::convert);
        }

        private TypeVariable createRootTypeVar(Label rootLabel) {
            Optional<TypeVariable> rootType = iterate(conjunction.variables())
                    .filter(Variable::isType).map(Variable::asType)
                    .filter(v -> v.label().isPresent() && v.label().get().properLabel().equals(rootLabel)).first();

            if (rootType.isPresent()) {
                TypeVariable convertedRootType = resolvers.register(rootType.get());
                neighbours.putIfAbsent(convertedRootType, new HashSet<>());
                return convertedRootType;
            }

            TypeVariable newRootType = new TypeVariable(Identifier.Variable.of(Reference.label(rootLabel.toString())));
            newRootType.label(rootLabel);
            neighbours.putIfAbsent(newRootType, new HashSet<>());
            return newRootType;
        }

        Resolvers resolvers() {
            return resolvers;
        }

        HashMap<TypeVariable, Set<TypeVariable>> neighbours() {
            return neighbours;
        }

        private boolean hasNoInfo(TypeVariable variable) {
            return (!variable.reference().isLabel() &&
                    !variable.sub().isPresent() &&
                    !variable.label().isPresent() &&
                    variable.is().isEmpty());
        }

        private TypeVariable convert(Variable variable) {
            if (resolvers.contains(variable)) return resolvers.resolver(variable);

            if (variable.isType()) return convert(variable.asType());
            else if (variable.isThing()) return convert(variable.asThing());
            else throw GraknException.of(ILLEGAL_STATE);
        }

        private TypeVariable convert(TypeVariable variable) {
            TypeVariable resolver = resolvers.register(variable);
            copyNeighbours(variable, resolver);
            neighbours.putIfAbsent(resolver, new HashSet<>());
            return resolver;
        }

        private TypeVariable convert(ThingVariable thingVariable) {
            TypeVariable resolver = resolvers.register(thingVariable);
            neighbours.putIfAbsent(resolver, new HashSet<>());

            if (thingVariable.constraints().isEmpty()) return resolver;

            thingVariable.isa().ifPresent(constraint -> convertIsa(resolver, constraint));
            thingVariable.is().forEach(constraint -> convertIs(resolver, constraint));
            thingVariable.has().forEach(constraint -> convertHas(resolver, constraint));
            thingVariable.value().forEach(constraint -> convertValue(resolver, constraint));
            thingVariable.relation().forEach(constraint -> convertRelation(resolver, constraint));
            return resolver;
        }

        private void convertRelation(TypeVariable owner, RelationConstraint relationConstraint) {
            if (hasNoInfo(owner)) addRootTypeVar(owner, rootRelationType);
            ThingVariable ownerThing = relationConstraint.owner();
            if (ownerThing.isa().isPresent()) {
                TypeVariable relationTypeVar = convert(ownerThing.isa().get().type());
                if (hasNoInfo(relationTypeVar)) addRootTypeVar(relationTypeVar, rootRelationType);
            }
            for (RelationConstraint.RolePlayer rolePlayer : relationConstraint.players()) {
                TypeVariable playerType = convert(rolePlayer.player());
                TypeVariable roleTypeVar = rolePlayer.roleType().orElse(null);

                if (roleTypeVar != null) {
                    roleTypeVar = convert(roleTypeVar);
                    if (hasNoInfo(roleTypeVar)) addRootTypeVar(roleTypeVar, rootRoleType);
                    addRelatesConstraint(owner, roleTypeVar);
                }

                if (roleTypeVar == null) {
                    TypeVariable resolveRolePlayer = resolvers.register(rolePlayer);
                    neighbours.put(resolveRolePlayer, new HashSet<>());
                    if (hasNoInfo(resolveRolePlayer)) addRootTypeVar(resolveRolePlayer, rootRoleType);
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
            TypeVariable rootConverted = resolvers.register(rootTypeVar);
            variable.sub(rootConverted, false);
            addNeighbours(variable, rootConverted);
        }

        private void convertHas(TypeVariable owner, HasConstraint hasConstraint) {
            TypeVariable attributeTypeVar = convert(hasConstraint.attribute());
            owner.owns(attributeTypeVar, null, false);
            if (hasNoInfo(attributeTypeVar)) addRootTypeVar(attributeTypeVar, rootAttributeType);
            addNeighbours(owner, attributeTypeVar);
            assert attributeTypeVar.sub().isPresent();
            addNeighbours(owner, attributeTypeVar.sub().get().type());
        }

        private void convertIsa(TypeVariable owner, IsaConstraint isaConstraint) {
            TypeVariable isaVar = convert(isaConstraint.type());
            if (!isaConstraint.isExplicit()) owner.sub(isaConstraint.type(), false);
            else if (isaConstraint.type().reference().isName()) owner.is(isaConstraint.type());
            else if (isaConstraint.type().label().isPresent())
                owner.label(isaConstraint.type().label().get().properLabel());
            else throw GraknException.of(ILLEGAL_STATE);
            addNeighbours(owner, isaVar);
        }

        private void convertIs(TypeVariable owner, grakn.core.pattern.constraint.thing.IsConstraint isConstraint) {
            TypeVariable isVar = convert(isConstraint.variable());
            owner.is(isVar);
            addNeighbours(owner, isVar);
        }

        private void convertValue(TypeVariable owner, ValueConstraint<?> constraint) {
            if (constraint.isBoolean()) owner.valueType(GraqlArg.ValueType.BOOLEAN);
            else if (constraint.isString()) owner.valueType(GraqlArg.ValueType.STRING);
            else if (constraint.isDateTime()) owner.valueType(GraqlArg.ValueType.DATETIME);
            else if (constraint.isDouble()) owner.valueType(GraqlArg.ValueType.DOUBLE);
            else if (constraint.isLong()) owner.valueType(GraqlArg.ValueType.LONG);
            else if (constraint.isVariable()) resolvers.register(constraint.asVariable().value());
            else throw GraknException.of(ILLEGAL_STATE);
            if (hasNoInfo(owner)) addRootTypeVar(owner, rootAttributeType);
        }

        private void addNeighbours(TypeVariable from, TypeVariable to) {
            neighbours.computeIfAbsent(from, k -> new HashSet<>()).add(to);
            neighbours.computeIfAbsent(to, k -> new HashSet<>()).add(from);
        }

        private void copyNeighbours(TypeVariable from, TypeVariable to) { // TODO: why is 'to' variable never used?
            for (TypeConstraint constraint : from.constraints()) {
                if (constraint.isSub()) addNeighbours(from, constraint.asSub().type());
                else if (constraint.isOwns()) addNeighbours(from, constraint.asOwns().attribute());
                else if (constraint.isPlays()) addNeighbours(from, constraint.asPlays().role());
                else if (constraint.isRelates()) addNeighbours(from, constraint.asRelates().role());
                else if (constraint.isIs()) addNeighbours(from, constraint.asIs().variable());
                else throw GraknException.of(ILLEGAL_STATE);
            }
        }
    }

    private static class Resolvers {

        private final Map<Identifier, TypeVariable> typeResolvers;
        private final Map<RelationConstraint.RolePlayer, TypeVariable> roleTypeResolvers;
        // TODO: Why is there no checkers and getters for 'roleTypeResolvers'?
        // TODO: do we still need 'roleTypeResolvers' given that we no longer put resolveRoleTypes on RelationConstraints?

        private int sysVarCounter;

        Resolvers() {
            this.sysVarCounter = 0;
            this.typeResolvers = new HashMap<>();
            this.roleTypeResolvers = new HashMap<>();
        }

        TypeVariable register(RelationConstraint.RolePlayer rolePlayer) {
            return roleTypeResolvers.computeIfAbsent(rolePlayer, rp -> new TypeVariable(newSystemVariable()));
        }

        TypeVariable register(Variable variable) {
            return typeResolvers.computeIfAbsent(variable.identifier(), id -> {
                TypeVariable newTypeVar;
                if (variable.reference().isAnonymous()) newTypeVar = new TypeVariable(newSystemVariable());
                else newTypeVar = new TypeVariable(variable.identifier());
                if (variable.isType()) newTypeVar.copyConstraints(variable.asType());
                return newTypeVar;
            });
        }

        private Identifier.Variable newSystemVariable() {
            return Identifier.Variable.of(SystemReference.of(sysVarCounter++));
        }

        boolean contains(Variable variable) {
            return typeResolvers.containsKey(variable.identifier());
        }

        Collection<TypeVariable> resolvers() {
            return typeResolvers.values();
        }

        TypeVariable resolver(Variable variable) {
            return typeResolvers.get(variable.identifier());
        }
    }
}
