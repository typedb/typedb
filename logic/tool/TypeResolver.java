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

package grakn.core.logic.tool;

import grakn.common.collection.Bytes;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Label;
import grakn.core.concept.ConceptManager;
import grakn.core.graph.common.Encoding;
import grakn.core.graph.vertex.TypeVertex;
import grakn.core.logic.LogicCache;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IIDConstraint;
import grakn.core.pattern.constraint.thing.IsConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.variable.SystemReference;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.Variable;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
import graql.lang.common.GraqlArg.ValueType;
import graql.lang.pattern.variable.Reference;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.UNSATISFIABLE_CONJUNCTION;
import static grakn.core.common.exception.ErrorMessage.ThingRead.THING_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.TypeRead.ROLE_TYPE_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static grakn.core.common.iterator.Iterators.iterate;
import static graql.lang.common.GraqlToken.Type.ATTRIBUTE;

public class TypeResolver {

    private final ConceptManager conceptMgr;
    private final TraversalEngine traversalEng;
    private final LogicCache logicCache;

    public TypeResolver(ConceptManager conceptMgr, TraversalEngine traversalEng, LogicCache logicCache) {
        this.conceptMgr = conceptMgr;
        this.traversalEng = traversalEng;
        this.logicCache = logicCache;
    }

    public Conjunction resolveLabels(Conjunction conjunction) {
        iterate(conjunction.variables()).filter(v -> v.isType() && v.asType().label().isPresent())
                .forEachRemaining(typeVar -> {
                    Label label = typeVar.asType().label().get().properLabel();
                    if (label.scope().isPresent()) {
                        String scope = label.scope().get();
                        Set<Label> labels = traversalEng.graph().schema().resolveRoleTypeLabels(label);
                        if (labels.isEmpty()) throw GraknException.of(ROLE_TYPE_NOT_FOUND, label.name(), scope);
                        typeVar.addResolvedTypes(labels);
                    } else {
                        TypeVertex type = traversalEng.graph().schema().getType(label);
                        if (type == null) throw GraknException.of(TYPE_NOT_FOUND, label);
                        typeVar.addResolvedType(label);
                    }
                });
        return conjunction;
    }

    public Conjunction resolve(Conjunction conjunction) {
        resolveLabels(conjunction);
        TraversalBuilder traversalConstructor = new TraversalBuilder(conjunction, conceptMgr);
        Map<Reference, Set<Label>> resolvedLabels = executeResolverTraversals(traversalConstructor);
        if (resolvedLabels.isEmpty()) {
            conjunction.setSatisfiable(false);
            return conjunction;
        }

        long numOfTypes = traversalEng.graph().schema().stats().thingTypeCount();
        long numOfConcreteTypes = traversalEng.graph().schema().stats().concreteThingTypeCount();

        resolvedLabels.forEach((ref, labels) -> {
            Variable variable = traversalConstructor.getVariable(ref);
            if (variable.isType() && labels.size() < numOfTypes ||
                    variable.isThing() && labels.size() < numOfConcreteTypes) {
                assert variable.resolvedTypes().isEmpty() || variable.resolvedTypes().containsAll(labels);
                variable.setResolvedTypes(labels);
            }
        });

        return conjunction;
    }

    private Map<Reference, Set<Label>> executeResolverTraversals(TraversalBuilder traversalConstructor) {
        return logicCache.resolver().get(traversalConstructor.traversal(), traversal -> {
            Map<Reference, Set<Label>> mapping = new HashMap<>();
            traversalEng.iterator(traversal).forEachRemaining(
                    result -> result.forEach((ref, vertex) -> {
                        mapping.putIfAbsent(ref, new HashSet<>());
                        assert vertex.isType();
                        // TODO: This filter should not be needed if we enforce traversal only to return non-abstract
                        if (!(vertex.asType().isAbstract() && traversalConstructor.getVariable(ref).isThing()))
                            mapping.get(ref).add(vertex.asType().properLabel());
                    })
            );
            return mapping;
        });
    }

    private static class TraversalBuilder {

        private final Map<Reference, Variable> variableRegister;
        private final Map<Identifier, TypeVariable> resolverRegister;
        private final Map<Identifier, Set<ValueType>> valueTypeRegister;
        private final ConceptManager conceptMgr;
        private final Traversal traversal;
        private int sysVarCounter;

        TraversalBuilder(Conjunction conjunction, ConceptManager conceptMgr) {
            this.conceptMgr = conceptMgr;
            this.traversal = new Traversal();
            this.variableRegister = new HashMap<>();
            this.resolverRegister = new HashMap<>();
            this.valueTypeRegister = new HashMap<>();
            this.sysVarCounter = 0;
            conjunction.variables().forEach(this::register);
        }

        Traversal traversal() {
            return traversal;
        }

        Variable getVariable(Reference reference) {
            return variableRegister.get(reference);
        }

        private void register(Variable variable) {
            if (variable.isType()) register(variable.asType());
            else if (variable.isThing()) register(variable.asThing());
            else throw GraknException.of(ILLEGAL_STATE);
        }

        private TypeVariable register(TypeVariable var) {
            if (resolverRegister.containsKey(var.id())) return resolverRegister.get(var.id());
            TypeVariable resolver;
            if (var.label().isPresent() && var.label().get().scope().isPresent()) {
                resolver = new TypeVariable(newSystemId());
                traversal.labels(resolver.id(), var.resolvedTypes());
            } else {
                resolver = var;
            }
            resolverRegister.put(var.id(), resolver);
            variableRegister.putIfAbsent(resolver.reference(), var);
            resolver.addTo(traversal);
            return resolver;
        }

        private TypeVariable register(ThingVariable var) {
            if (resolverRegister.containsKey(var.id())) return resolverRegister.get(var.id());

            TypeVariable resolver = new TypeVariable(var.reference().isAnonymous() ? newSystemId() : var.id());
            resolverRegister.put(var.id(), resolver);
            variableRegister.putIfAbsent(resolver.reference(), var);
            valueTypeRegister.putIfAbsent(resolver.id(), set());

            // Note: order is important! convertValue assumes that any other Variable encountered from that edge will
            // have resolved its valueType, so we execute convertValue first.
            var.value().forEach(constraint -> registerValue(resolver, constraint));
            var.isa().ifPresent(constraint -> registerIsa(resolver, constraint));
            var.is().forEach(constraint -> registerIs(resolver, constraint));
            var.has().forEach(constraint -> registerHas(resolver, constraint));
            var.relation().forEach(constraint -> registerRelation(resolver, constraint));
            var.iid().ifPresent(constraint -> registerIID(resolver, constraint));
            return resolver;
        }

        private void registerIID(TypeVariable owner, IIDConstraint iidConstraint) {
            if (conceptMgr.getThing(iidConstraint.iid()) == null)
                throw GraknException.of(THING_NOT_FOUND, Bytes.bytesToHexString(iidConstraint.iid()));
            traversal.labels(owner.id(), conceptMgr.getThing(iidConstraint.iid()).getType().getLabel());
        }

        private void registerIsa(TypeVariable owner, IsaConstraint isaConstraint) {
            if (!isaConstraint.isExplicit())
                traversal.sub(owner.id(), register(isaConstraint.type()).id(), true);
            else if (isaConstraint.type().reference().isName())
                traversal.equalTypes(owner.id(), register(isaConstraint.type()).id());
            else if (isaConstraint.type().label().isPresent())
                traversal.labels(owner.id(), isaConstraint.type().label().get().properLabel());
            else throw GraknException.of(ILLEGAL_STATE);
        }

        private void registerIs(TypeVariable owner, IsConstraint isConstraint) {
            traversal.equalTypes(owner.id(), register(isConstraint.variable()).id());
        }

        private void registerHas(TypeVariable owner, HasConstraint hasConstraint) {
            traversal.owns(owner.id(), register(hasConstraint.attribute()).id(), false);
        }

        private void registerRelation(TypeVariable owner, RelationConstraint constraint) {
            for (RelationConstraint.RolePlayer rolePlayer : constraint.players()) {
                TypeVariable playerResolver = register(rolePlayer.player());
                TypeVariable actingRoleResolver = register(new TypeVariable(newSystemId()));
                if (rolePlayer.roleType().isPresent()) {
                    TypeVariable roleTypeResolver = register(rolePlayer.roleType().get());
                    traversal.sub(actingRoleResolver.id(), roleTypeResolver.id(), true);
                }
                traversal.relates(owner.id(), actingRoleResolver.id());
                traversal.plays(playerResolver.id(), actingRoleResolver.id());
            }
        }

        private void registerValue(TypeVariable owner, ValueConstraint<?> constraint) {
            Set<ValueType> valueTypes;
            if (constraint.isVariable()) {
                TypeVariable comparableVar = register(constraint.asVariable().value());
                assert valueTypeRegister.containsKey(comparableVar.id()); //This will fail without careful ordering.
                registerSubAttribute(comparableVar.id());
                valueTypes = valueTypeRegister.get(comparableVar.id());
            } else {
                valueTypes = iterate(Encoding.ValueType.of(constraint.value().getClass()).comparables())
                        .map(Encoding.ValueType::graqlValueType).toSet();
            }

            if (valueTypeRegister.get(owner.id()).isEmpty()) {
                valueTypes.forEach(valueType -> traversal.valueType(owner.id(), valueType));
                valueTypeRegister.put(owner.id(), valueTypes);
            } else if (!valueTypeRegister.get(owner.id()).containsAll(valueTypes)) {
                throw GraknException.of(UNSATISFIABLE_CONJUNCTION, constraint);
            }
            registerSubAttribute(owner.id());
        }

        private void registerSubAttribute(Identifier.Variable variable) {
            Identifier.Variable attributeID = Identifier.Variable.of(Reference.label(ATTRIBUTE.toString()));
            traversal.labels(attributeID, Label.of(ATTRIBUTE.toString()));
            traversal.sub(variable, attributeID, true);
        }

        private Identifier.Variable newSystemId() {
            return Identifier.Variable.of(SystemReference.of(sysVarCounter++));
        }
    }
}
