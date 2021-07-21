/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.logic.tool;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.graph.vertex.Vertex;
import com.vaticle.typedb.core.logic.LogicCache;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.pattern.Negation;
import com.vaticle.typedb.core.pattern.constraint.thing.HasConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.IIDConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.IsConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.IsaConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.RelationConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.ValueConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.LabelConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.OwnsConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.PlaysConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.RegexConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.RelatesConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.SubConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.TypeConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.ValueTypeConstraint;
import com.vaticle.typedb.core.pattern.variable.SystemReference;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.VertexMap;
import com.vaticle.typeql.lang.common.TypeQLArg.ValueType;
import com.vaticle.typeql.lang.pattern.variable.Reference;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.UNSATISFIABLE_PATTERN;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.ROLE_TYPE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typeql.lang.common.TypeQLToken.Type.ATTRIBUTE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Type.THING;

public class TypeResolver {

    private final ConceptManager conceptMgr;
    private final TraversalEngine traversalEng;
    private final LogicCache logicCache;

    public TypeResolver(LogicCache logicCache, TraversalEngine traversalEng, ConceptManager conceptMgr) {
        this.conceptMgr = conceptMgr;
        this.traversalEng = traversalEng;
        this.logicCache = logicCache;
    }

    public FunctionalIterator<Map<Identifier.Variable.Name, Label>> namedCombinations(Conjunction conjunction, boolean insertable) {
        GraphTraversal resolverTraversal = new GraphTraversal();
        TraversalBuilder traversalBuilder = new TraversalBuilder(conjunction, conceptMgr, resolverTraversal, 0, insertable);
        resolverTraversal.filter(traversalBuilder.retrievedResolvers());
        return traversalEng.iterator(traversalBuilder.traversal()).map(vertexMap -> {
            Map<Identifier.Variable.Name, Label> mapping = new HashMap<>();
            vertexMap.forEach((id, vertex) -> {
                assert vertex.isType();
                traversalBuilder.getOriginalVariable(id).map(Variable::id).filter(Identifier::isName)
                        .ifPresent(originalRef -> mapping.put(originalRef.asName(), vertex.asType().properLabel()));
            });
            return mapping;
        });
    }

    public void resolveVariableLabels(Conjunction conjunction) {
        iterate(conjunction.variables()).filter(v -> v.isType() && v.asType().label().isPresent())
                .forEachRemaining(typeVar -> {
                    Label label = typeVar.asType().label().get().properLabel();
                    if (label.scope().isPresent()) {
                        String scope = label.scope().get();
                        Set<Label> labels = traversalEng.graph().schema().resolveRoleTypeLabels(label);
                        if (labels.isEmpty()) throw TypeDBException.of(ROLE_TYPE_NOT_FOUND, label.name(), scope);
                        typeVar.addResolvedTypes(labels);
                    } else {
                        TypeVertex type = traversalEng.graph().schema().getType(label);
                        if (type == null) throw TypeDBException.of(TYPE_NOT_FOUND, label);
                        typeVar.addResolvedType(label);
                    }
                });
    }

    public void resolve(Disjunction disjunction) {
        resolve(disjunction, new LinkedList<>());
    }

    private void resolve(Disjunction disjunction, List<Conjunction> scopingConjunctions) {
        disjunction.conjunctions().forEach(conjunction -> resolve(conjunction, scopingConjunctions));
    }

    private void resolve(Conjunction conjunction, List<Conjunction> scopingConjunctions) {
        resolveVariables(conjunction, scopingConjunctions, false);
        for (Negation negation : conjunction.negations()) {
            resolve(negation.disjunction(), list(scopingConjunctions, conjunction));
        }
    }

    public void resolveVariables(Conjunction conjunction, boolean insertable) {
        resolveVariables(conjunction, list(), insertable);
    }

    private void resolveVariables(Conjunction conjunction, List<Conjunction> scopingConjunctions, boolean insertable) {
        resolveVariableLabels(conjunction);
        if (!isSchemaQuery(conjunction)) resolveVariableTypes(conjunction, scopingConjunctions, insertable);
    }

    private void resolveVariableTypes(Conjunction conjunction, List<Conjunction> scopingConjunctions, boolean insertable) {
        GraphTraversal resolverTraversal = new GraphTraversal();
        TraversalBuilder traversalBuilder = builder(resolverTraversal, conjunction, scopingConjunctions, insertable);
        resolverTraversal.filter(traversalBuilder.retrievedResolvers());
        Map<Identifier.Variable.Retrievable, Set<Label>> resolvedLabels = executeTypeResolvers(traversalBuilder);
        if (resolvedLabels.isEmpty()) conjunction.setCoherent(false);
        else {
            resolvedLabels.forEach((id, labels) -> traversalBuilder.getOriginalVariable(id).ifPresent(variable -> {
                assert variable.resolvedTypes().isEmpty() || variable.resolvedTypes().containsAll(labels);
                variable.setResolvedTypes(labels);
            }));
        }
    }

    private boolean isSchemaQuery(Conjunction conjunction) {
        return iterate(conjunction.variables()).noneMatch(Variable::isThing);
    }

    private TraversalBuilder builder(GraphTraversal traversal, Conjunction conjunction, List<Conjunction> scopingConjunctions,
                                     boolean insertable) {
        TraversalBuilder currentBuilder;
        if (!scopingConjunctions.isEmpty()) {
            Set<Reference.Name> names = iterate(conjunction.variables()).filter(v -> v.reference().isName())
                    .map(v -> v.reference().asName()).toSet();
            currentBuilder = new TraversalBuilder(scopingConjunctions.get(0), conceptMgr, traversal, 0, insertable);
            for (int i = 1; i < scopingConjunctions.size(); i++) {
                Conjunction scoping = scopingConjunctions.get(i);
                if (iterate(scoping.variables()).noneMatch(v -> v.reference().isName() && names.contains(v.reference().asName()))) {
                    // skip any scoping conjunctions without a named variable in common
                    continue;
                }
                currentBuilder = new TraversalBuilder(scoping, conceptMgr, traversal, currentBuilder.sysVarCounter(), insertable);
            }
            currentBuilder = new TraversalBuilder(conjunction, conceptMgr, traversal, currentBuilder.sysVarCounter(), insertable);
        } else {
            currentBuilder = new TraversalBuilder(conjunction, conceptMgr, traversal, 0, insertable);
        }
        return currentBuilder;
    }

    private Map<Identifier.Variable.Retrievable, Set<Label>> executeTypeResolvers(TraversalBuilder traversalBuilder) {
        return logicCache.resolver().get(traversalBuilder.traversal().structure(), structure -> {
            Map<Identifier.Variable.Retrievable, Set<Label>> mapping = new HashMap<>();
            traversalEng.iterator(traversalBuilder.traversal(), true)
                    // TODO: This filter should not be needed if we enforce traversal only to return non-abstract
                    .filter(result -> !containsAbstractThing(result, traversalBuilder))
                    .forEachRemaining(
                            result -> {
                                assert iterate(result.map().values()).allMatch(Vertex::isType);
                                result.forEach((id, vertex) -> {
                                    Optional<Variable> originalVar = traversalBuilder.getOriginalVariable(id);
                                    if (originalVar.isPresent()) {
                                        Set<Label> labels = mapping.computeIfAbsent(id, (i) -> new HashSet<>());
                                        labels.add(vertex.asType().properLabel());
                                    }
                                });
                            }
                    );
            return mapping;
        });
    }

    private boolean containsAbstractThing(VertexMap resolvedTypes, TraversalBuilder traversalBuilder) {
        for (Map.Entry<Identifier.Variable.Retrievable, Vertex<?, ?>> entry : resolvedTypes.map().entrySet()) {
            Identifier.Variable.Retrievable id = entry.getKey();
            Optional<Variable> var = traversalBuilder.getOriginalVariable(id);
            if (entry.getValue().asType().isAbstract() && var.isPresent() && var.get().isThing()) return true;
        }
        return false;
    }

    private static class TraversalBuilder {

        private static final Identifier.Variable ROOT_ATTRIBUTE_ID = Identifier.Variable.label(ATTRIBUTE.toString());
        private static final Identifier.Variable ROOT_THING_ID = Identifier.Variable.label(THING.toString());
        private static final Label ROOT_ATTRIBUTE_LABEL = Label.of(ATTRIBUTE.toString());
        private static final Label ROOT_THING_LABEL = Label.of(THING.toString());
        private final Map<Identifier.Variable, Set<ValueType>> resolverValueTypes;
        private final Map<Identifier.Variable, TypeVariable> originalToResolver;
        private final Map<Identifier.Variable, Variable> resolverToOriginal;
        private final ConceptManager conceptMgr;
        private final Conjunction conjunction;
        private final GraphTraversal traversal;
        private final boolean insertable;
        private boolean hasRootAttribute;
        private boolean hasRootThing;
        private int sysVarCounter;

        TraversalBuilder(Conjunction conjunction, ConceptManager conceptMgr, GraphTraversal initialTraversal,
                         int initialAnonymousVarCounter, boolean insertable) {
            this.conceptMgr = conceptMgr;
            this.conjunction = conjunction;
            this.traversal = initialTraversal;
            this.resolverToOriginal = new HashMap<>();
            this.originalToResolver = new HashMap<>();
            this.resolverValueTypes = new HashMap<>();
            this.sysVarCounter = initialAnonymousVarCounter;
            this.insertable = insertable;
            this.hasRootAttribute = false;
            this.hasRootThing  = false;
            conjunction.variables().forEach(this::register);
        }

        public Set<Identifier.Variable.Retrievable> retrievedResolvers() {
            return iterate(resolverToOriginal.keySet()).filter(Identifier::isRetrievable).map(Identifier.Variable::asRetrievable).toSet();
        }

        public int sysVarCounter() {
            return sysVarCounter;
        }

        GraphTraversal traversal() {
            return traversal;
        }

        Optional<Variable> getOriginalVariable(Identifier.Variable id) {
            return Optional.ofNullable(resolverToOriginal.get(id));
        }

        private void register(Variable variable) {
            if (variable.isType()) register(variable.asType());
            else if (variable.isThing()) register(variable.asThing());
            else throw TypeDBException.of(ILLEGAL_STATE);
        }

        private TypeVariable register(TypeVariable var) {
            if (originalToResolver.containsKey(var.id())) return originalToResolver.get(var.id());
            TypeVariable resolver;
            if (var.label().isPresent() && var.label().get().scope().isPresent()) {
                resolver = new TypeVariable(newSystemId());
                traversal.labels(resolver.id(), var.resolvedTypes());
            } else {
                resolver = var;
            }
            originalToResolver.put(var.id(), resolver);
            resolverToOriginal.putIfAbsent(resolver.id(), var);
            if (!var.resolvedTypes().isEmpty()) traversal.labels(resolver.id(), var.resolvedTypes());

            for (TypeConstraint constraint : var.constraints()) {
                if (constraint.isAbstract()) registerAbstract(resolver);
                else if (constraint.isIs()) registerIsType(resolver, constraint.asIs());
                else if (constraint.isOwns()) registerOwns(resolver, constraint.asOwns());
                else if (constraint.isPlays()) registerPlays(resolver, constraint.asPlays());
                else if (constraint.isRegex()) registerRegex(resolver, constraint.asRegex());
                else if (constraint.isRelates()) registerRelates(resolver, constraint.asRelates());
                else if (constraint.isSub()) registerSub(resolver, constraint.asSub());
                else if (constraint.isValueType()) registerValueType(resolver, constraint.asValueType());
                else if (!constraint.isLabel()) throw TypeDBException.of(ILLEGAL_STATE);
            }

            return resolver;
        }

        private void registerAbstract(TypeVariable resolver) {
            traversal.isAbstract(resolver.id());
        }

        private void registerIsType(TypeVariable resolver, com.vaticle.typedb.core.pattern.constraint.type.IsConstraint isConstraint) {
            traversal.equalTypes(resolver.id(), register(isConstraint.variable()).id());
        }

        private void registerOwns(TypeVariable resolver, OwnsConstraint ownsConstraint) {
            traversal.owns(resolver.id(), register(ownsConstraint.attribute()).id(), ownsConstraint.isKey());
        }

        private void registerPlays(TypeVariable resolver, PlaysConstraint playsConstraint) {
            traversal.plays(resolver.id(), register(playsConstraint.role()).id());
        }

        private void registerRegex(TypeVariable resolver, RegexConstraint regexConstraint) {
            traversal.regex(resolver.id(), regexConstraint.regex().pattern());
        }

        private void registerRelates(TypeVariable resolver, RelatesConstraint relatesConstraint) {
            traversal.relates(resolver.id(), register(relatesConstraint.role()).id());
        }

        private void registerSub(TypeVariable resolver, SubConstraint subConstraint) {
            traversal.sub(resolver.id(), register(subConstraint.type()).id(), !subConstraint.isExplicit());
        }

        private void registerValueType(TypeVariable resolver, ValueTypeConstraint valueTypeConstraint) {
            traversal.valueType(resolver.id(), valueTypeConstraint.valueType());
        }

        private TypeVariable register(ThingVariable var) {
            if (originalToResolver.containsKey(var.id())) return originalToResolver.get(var.id());

            TypeVariable resolver = new TypeVariable(var.id());
            originalToResolver.put(var.id(), resolver);
            resolverToOriginal.putIfAbsent(resolver.id(), var);
            resolverValueTypes.putIfAbsent(resolver.id(), set());

            // Note: order is important! convertValue assumes that any other Variable encountered from that edge will
            // have resolved its valueType, so we execute convertValue first.
            var.value().forEach(constraint -> registerValue(resolver, constraint));
            var.isa().ifPresent(constraint -> registerIsa(resolver, constraint));
            var.is().forEach(constraint -> registerIs(resolver, constraint));
            var.has().forEach(constraint -> registerHas(resolver, constraint));
            if (insertable) var.relation().ifPresent(constraint -> registerInsertableRelation(resolver, constraint));
            else var.relation().ifPresent(constraint -> registerRelation(resolver, constraint));
            var.iid().ifPresent(constraint -> {
                if (resolver.constraints().isEmpty()) registerSubThing(resolver);
            });
            return resolver;
        }

        private void registerSubThing(TypeVariable resolver) {
            assert resolver.constraints().isEmpty();
            registerRootThing();
            traversal.sub(resolver.id(), ROOT_THING_ID, true);
        }

        private void registerIID(TypeVariable resolver, IIDConstraint iidConstraint) {
            Thing thing = conceptMgr.getThing(iidConstraint.iid());
            if (thing != null) traversal.labels(resolver.id(), thing.getType().getLabel());
        }

        private void registerIsa(TypeVariable resolver, IsaConstraint isaConstraint) {
            if (!isaConstraint.isExplicit() && !insertable) {
                traversal.sub(resolver.id(), register(isaConstraint.type()).id(), true);
            } else if (isaConstraint.type().reference().isName()) {
                traversal.equalTypes(resolver.id(), register(isaConstraint.type()).id());
            } else if (isaConstraint.type().label().isPresent()) {
                traversal.labels(resolver.id(), isaConstraint.type().label().get().properLabel());
            } else {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
        }

        private void registerIs(TypeVariable resolver, IsConstraint isConstraint) {
            traversal.equalTypes(resolver.id(), register(isConstraint.variable()).id());
        }

        private void registerHas(TypeVariable resolver, HasConstraint hasConstraint) {
            TypeVariable attributeResolver = register(hasConstraint.attribute());
            traversal.owns(resolver.id(), attributeResolver.id(), false);
            registerSubAttribute(attributeResolver);
        }

        private void registerRelation(TypeVariable resolver, RelationConstraint constraint) {
            for (RelationConstraint.RolePlayer rolePlayer : constraint.players()) {
                TypeVariable playerResolver = register(rolePlayer.player());
                TypeVariable actingRoleResolver = new TypeVariable(newSystemId());
                if (rolePlayer.roleType().isPresent()) {
                    TypeVariable roleTypeResolver = register(rolePlayer.roleType().get());
                    traversal.sub(actingRoleResolver.id(), roleTypeResolver.id(), true);
                }
                traversal.relates(resolver.id(), actingRoleResolver.id());
                traversal.plays(playerResolver.id(), actingRoleResolver.id());
            }
        }

        private void registerInsertableRelation(TypeVariable resolver, RelationConstraint constraint) {
            for (RelationConstraint.RolePlayer rolePlayer : constraint.players()) {
                TypeVariable playerResolver = register(rolePlayer.player());
                TypeVariable roleResolver = register(rolePlayer.roleType().isPresent() ?
                                                             rolePlayer.roleType().get() : new TypeVariable(newSystemId()));
                traversal.relates(resolver.id(), roleResolver.id());
                traversal.plays(playerResolver.id(), roleResolver.id());
            }
        }

        private void registerValue(TypeVariable resolver, ValueConstraint<?> constraint) {
            Set<ValueType> valueTypes;
            if (constraint.isVariable()) {
                TypeVariable comparableVar = register(constraint.asVariable().value());
                assert resolverValueTypes.containsKey(comparableVar.id()); //This will fail without careful ordering.
                registerSubAttribute(comparableVar);
                valueTypes = resolverValueTypes.get(comparableVar.id());
            } else {
                valueTypes = iterate(Encoding.ValueType.of(constraint.value().getClass()).comparables())
                        .map(Encoding.ValueType::typeQLValueType).toSet();
            }

            if (resolverValueTypes.get(resolver.id()).isEmpty()) {
                valueTypes.forEach(valueType -> traversal.valueType(resolver.id(), valueType));
                resolverValueTypes.put(resolver.id(), valueTypes);
            } else if (!resolverValueTypes.get(resolver.id()).containsAll(valueTypes)) {
                // TODO this is a bit odd - can we set not coherent here and short circuit?
                throw TypeDBException.of(UNSATISFIABLE_PATTERN, conjunction, constraint);
            }
            registerSubAttribute(resolver);
        }

        private void registerSubAttribute(Variable resolver) {
            assert resolverToOriginal.get(resolver.id()).isThing();
            Optional<IsaConstraint> isa = resolverToOriginal.get(resolver.id()).asThing().isa();
            if (!isa.isPresent()) {
                registerRootAttribute();
                traversal.sub(resolver.id(), ROOT_ATTRIBUTE_ID, true);
            } else {
                Optional<LabelConstraint> labelCons = isa.get().type().label();
                if (labelCons.isPresent() && !labelCons.get().properLabel().equals(ROOT_ATTRIBUTE_LABEL) &&
                        !labelCons.get().properLabel().equals(ROOT_THING_LABEL)) {
                    registerRootAttribute();
                    traversal.sub(register(isa.get().type()).id(), ROOT_ATTRIBUTE_ID, true);
                }
            }
        }

        private void registerRootAttribute() {
            if (!hasRootAttribute) {
                traversal.labels(ROOT_ATTRIBUTE_ID, ROOT_ATTRIBUTE_LABEL);
                hasRootAttribute = true;
            }
        }

        private void registerRootThing() {
            if (!hasRootThing) {
                traversal.labels(ROOT_THING_ID, ROOT_THING_LABEL);
                hasRootThing = true;
            }
        }


        private Identifier.Variable newSystemId() {
            return Identifier.Variable.of(SystemReference.of(sysVarCounter++));
        }
    }
}
