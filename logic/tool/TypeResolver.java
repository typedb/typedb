/*
 * Copyright (C) 2020 Grakn Labs
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.logic.tool;

import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Label;
import grakn.core.concept.ConceptManager;
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

import static grakn.common.collection.Collections.map;
import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.TypeRead.ROLE_TYPE_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static grakn.core.common.iterator.Iterators.iterate;

//TODO: here we remake Type Resolver, using a Traversal Structure instead of a Pattern to move on the graph and find out answers.
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

    public Conjunction resolveVariables(Conjunction conjunction) {
        resolveLabels(conjunction);
        TraversalConstructor traversalConstructor = new TraversalConstructor(conjunction, conceptMgr);

        if (traversalConstructor.isSatisfiable) {
            Map<Reference, Set<Label>> resolvedLabels = runTraversalEngine(traversalConstructor);
            resolvedLabels.forEach((ref, labels) -> traversalConstructor.getVariable(ref).addResolvedTypes(labels));
            findVariablesWithNoHints(conjunction);
        } else {
            iterate(conjunction.variables()).forEachRemaining(variable -> variable.setSatisfiable(false));
        }

        return conjunction;
    }

    private void findVariablesWithNoHints(Conjunction conjunction) {
        long numOfTypes = traversalEng.graph().schema().stats().thingTypeCount();
        long numOfConcreteTypes = traversalEng.graph().schema().stats().concreteThingTypeCount();
        iterate(conjunction.variables()).filter(variable -> (
                variable.isType() && variable.resolvedTypes().size() == numOfTypes ||
                        variable.isThing() && variable.resolvedTypes().size() == numOfConcreteTypes
        )).forEachRemaining(Variable::clearResolvedTypes);
    }

    private Map<Reference, Set<Label>> runTraversalEngine(TraversalConstructor traversalConstructor) {
        return logicCache.resolverTraversal().get(traversalConstructor.resolverTraversal(), traversal -> {
            Map<Reference, Set<Label>> mapping = new HashMap<>();
            traversalEng.iterator(traversal).forEachRemaining(
                    result -> result.forEach((ref, vertex) -> {
                        mapping.putIfAbsent(ref, new HashSet<>());
                        assert vertex.isType();
                        if (!(vertex.asType().isAbstract() && traversalConstructor.getVariable(ref).isThing()))
                            mapping.get(ref).add(Label.of(vertex.asType().label(), vertex.asType().scope()));
                    })
            );
            return mapping;
        });
    }

    private static class TraversalConstructor {

        private final Map<Reference, Variable> variableRegister;
        private final Map<Identifier, TypeVariable> resolverRegister;
        private final Traversal resolverTraversal;
        private int sysVarCounter;
        private final ConceptManager conceptMgr;
        private final Map<Identifier, Set<ValueType>> valueTypeRegister;
        private boolean isSatisfiable;

        TraversalConstructor(Conjunction conjunction, ConceptManager conceptMgr) {
            this.conceptMgr = conceptMgr;
            this.resolverTraversal = new Traversal();
            this.variableRegister = new HashMap<>();
            this.resolverRegister = new HashMap<>();
            this.sysVarCounter = 0;
            this.isSatisfiable = true;
            this.valueTypeRegister = new HashMap<>();
            conjunction.variables().forEach(this::convert);
        }

        Traversal resolverTraversal() {
            return resolverTraversal;
        }

        Variable getVariable(Reference reference) {
            return variableRegister.get(reference);
        }

        private void convert(Variable variable) {
            if (variable.isType()) convert(variable.asType());
            else if (variable.isThing()) convert(variable.asThing());
            else throw GraknException.of(ILLEGAL_STATE);
        }

        private TypeVariable convert(TypeVariable variable) {
            if (resolverRegister.containsKey(variable.id())) return resolverRegister.get(variable.id());
            resolverRegister.put(variable.id(), variable);
            variableRegister.putIfAbsent(variable.reference(), variable);
            variable.addTo(resolverTraversal);
            return variable;
        }

        private TypeVariable convert(ThingVariable variable) {
            if (resolverRegister.containsKey(variable.id())) return resolverRegister.get(variable.id());

            TypeVariable resolver = new TypeVariable(variable.reference().isAnonymous() ?
                                                             newSystemId() : variable.id());
            resolverRegister.put(variable.id(), resolver);
            variableRegister.putIfAbsent(resolver.reference(), variable);
            valueTypeRegister.putIfAbsent(resolver.id(), set());

            //Note: order is important!
            variable.isa().ifPresent(constraint -> convertIsa(resolver, constraint));
            variable.value().forEach(constraint -> convertValue(resolver, constraint));
            variable.is().forEach(constraint -> convertIs(resolver, constraint));
            variable.has().forEach(constraint -> convertHas(resolver, constraint));
            variable.relation().forEach(constraint -> convertRelation(resolver, constraint));
            variable.iid().ifPresent(constraint -> convertIID(resolver, constraint));
            return resolver;
        }

        private void convertIID(TypeVariable owner, IIDConstraint iidConstraint) {
            assert conceptMgr.getThing(iidConstraint.iid()) != null;
            resolverTraversal.labels(owner.id(), conceptMgr.getThing(iidConstraint.iid()).getType().getLabel());
        }

        private void convertIsa(TypeVariable owner, IsaConstraint isaConstraint) {
            if (!isaConstraint.isExplicit()) resolverTraversal.sub(owner.id(), convert(isaConstraint.type()).id(), true);
            else if (isaConstraint.type().reference().isName())
                resolverTraversal.equalTypes(owner.id(), convert(isaConstraint.type()).id());
            else if (isaConstraint.type().label().isPresent())
                resolverTraversal.labels(owner.id(), isaConstraint.type().label().get().properLabel());
        }

        private void convertIs(TypeVariable owner, IsConstraint isConstraint) {
            resolverTraversal.equalTypes(owner.id(), convert(isConstraint.variable()).id());
        }

        private void convertHas(TypeVariable owner, HasConstraint hasConstraint) {
            resolverTraversal.owns(owner.id(), convert(hasConstraint.attribute()).id(), false);
        }

        private void convertValue(TypeVariable owner, ValueConstraint<?> constraint) {
            Set<ValueType> valueTypes = findComparableValueTypes(constraint);
            if (valueTypeRegister.get(owner.id()).isEmpty()) {
                valueTypes.forEach(valueType -> resolverTraversal.valueType(owner.id(), valueType));
                valueTypeRegister.put(owner.id(), valueTypes);
            } else if (!valueTypeRegister.get(owner.id()).containsAll(valueTypes)) this.isSatisfiable = false;
            subAttribute(owner.id());
        }

        private Set<ValueType> findComparableValueTypes(ValueConstraint<?> constraint) {
            if (constraint.isBoolean()) return set(ValueType.BOOLEAN);
            else if (constraint.isString()) return set(ValueType.STRING);
            else if (constraint.isDateTime()) return set(ValueType.DATETIME);
            else if (constraint.isLong() || constraint.isDouble()) return set(ValueType.LONG, ValueType.DOUBLE);
            else if (constraint.isVariable()) {
                TypeVariable comparableVar = convert(constraint.asVariable().value());
                assert valueTypeRegister.containsKey(comparableVar.id());
                subAttribute(comparableVar.id());
                return valueTypeRegister.get(comparableVar.id());
            } else throw GraknException.of(ILLEGAL_STATE);
        }

        private void subAttribute(Identifier.Variable variable) {
            Identifier.Variable attributeID = Identifier.Variable.of(Reference.label("attribute"));
            resolverTraversal.labels(attributeID, Label.of("attribute"));
            resolverTraversal.sub(variable, attributeID, true);
        }

        private void convertRelation(TypeVariable owner, RelationConstraint constraint) {
            for (RelationConstraint.RolePlayer rolePlayer : constraint.players()) {
                TypeVariable playerType = convert(rolePlayer.player());
                TypeVariable playingRoleType = convert(new TypeVariable(newSystemId()));
                if (rolePlayer.roleType().isPresent()) {
                    TypeVariable roleTypeVar = convert(rolePlayer.roleType().get());
                    resolverTraversal.sub(playingRoleType.id(), roleTypeVar.id(), true);
                }
                resolverTraversal.relates(owner.id(), playingRoleType.id());
                resolverTraversal.plays(playerType.id(), playingRoleType.id());
            }
        }

        private Identifier.Variable newSystemId() {
            return Identifier.Variable.of(SystemReference.of(sysVarCounter++));
        }

    }


}
