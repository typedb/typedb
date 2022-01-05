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
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.iid.VertexIID;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
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
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Name;
import com.vaticle.typedb.core.traversal.graph.TraversalVertex;
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

public class TypeInference {

    private final TraversalEngine traversalEng;
    private final LogicCache logicCache;
    private final GraphManager graphMgr;

    public TypeInference(LogicCache logicCache, TraversalEngine traversalEng, GraphManager graphMgr) {
        this.traversalEng = traversalEng;
        this.logicCache = logicCache;
        this.graphMgr = graphMgr;
    }

    public FunctionalIterator<Map<Name, Label>> typePermutations(Conjunction conjunction, boolean insertable) {
        TraversalBuilder traversalBuilder = new TraversalBuilder(conjunction, insertable, graphMgr);
        GraphTraversal.Type traversal = traversalBuilder.traversal();
        traversal.filter(traversalBuilder.retrievedResolvers());
        return traversalEng.iterator(traversal).map(vertexMap -> {
            Map<Name, Label> mapping = new HashMap<>();
            vertexMap.forEach((id, vertex) -> {
                assert vertex.isType();
                traversalBuilder.getOriginalVariable(id).map(Variable::id).filter(Identifier::isName)
                        .ifPresent(originalRef -> mapping.put(originalRef.asName(), vertex.asType().properLabel()));
            });
            return mapping;
        });
    }

    public void propagateLabels(Conjunction conj) {
        iterate(conj.variables()).filter(v -> v.isType() && v.asType().label().isPresent()).forEachRemaining(typeVar -> {
            Label label = typeVar.asType().label().get().properLabel();
            if (label.scope().isPresent()) {
                String scope = label.scope().get();
                Set<Label> labels = traversalEng.graph().schema().resolveRoleTypeLabels(label);
                if (labels.isEmpty()) throw TypeDBException.of(ROLE_TYPE_NOT_FOUND, label.name(), scope);
                typeVar.addInferredTypes(labels);
            } else {
                TypeVertex type = traversalEng.graph().schema().getType(label);
                if (type == null) throw TypeDBException.of(TYPE_NOT_FOUND, label);
                typeVar.addInferredTypes(label);
            }
        });
    }

    public void infer(Disjunction disjunction) {
        infer(disjunction, new LinkedList<>());
    }

    private void infer(Disjunction disjunction, List<Conjunction> scopingConjunctions) {
        disjunction.conjunctions().forEach(conjunction -> {
            infer(conjunction, scopingConjunctions, false);
            for (Negation negation : conjunction.negations()) {
                infer(negation.disjunction(), list(scopingConjunctions, conjunction));
            }
        });
    }

    public void infer(Conjunction conjunction, boolean insertable) {
        infer(conjunction, list(), insertable);
    }

    private void infer(Conjunction conjunction, List<Conjunction> scopingConjunctions, boolean insertable) {
        propagateLabels(conjunction);
        if (isSchemaQuery(conjunction)) return;

        TraversalBuilder builder = builder(conjunction, scopingConjunctions, graphMgr, insertable);
        Optional<Map<Identifier.Variable.Retrievable, Set<Label>>> resolvedLabels = executeTypeResolvers(builder);
        if (resolvedLabels.isEmpty()) conjunction.setCoherent(false);
        else {
            resolvedLabels.get().forEach((id, labels) -> builder.getOriginalVariable(id).ifPresent(variable -> {
                assert variable.inferredTypes().isEmpty() || variable.inferredTypes().containsAll(labels);
                variable.setInferredTypes(labels);
            }));
        }
    }

    private boolean isSchemaQuery(Conjunction conjunction) {
        return iterate(conjunction.variables()).noneMatch(Variable::isThing);
    }

    private TraversalBuilder builder(Conjunction conjunction, List<Conjunction> scopingConjunctions, GraphManager graphMgr, boolean insertable) {
        TraversalBuilder currentBuilder = null;
        if (!scopingConjunctions.isEmpty()) {
            Set<Reference.Name> names = iterate(conjunction.variables()).filter(v -> v.reference().isName())
                    .map(v -> v.reference().asName()).toSet();
            for (Conjunction scoping : scopingConjunctions) {
                // only include conjunctions with a variable in common
                if (iterate(scoping.variables()).anyMatch(v -> v.reference().isName() && names.contains(v.reference().asName()))) {
                    if (currentBuilder == null) currentBuilder = new TraversalBuilder(scoping, insertable, graphMgr);
                    else currentBuilder = new TraversalBuilder(scoping, currentBuilder, insertable, graphMgr);
                }
            }
            currentBuilder = new TraversalBuilder(conjunction, currentBuilder, insertable, graphMgr);
        } else currentBuilder = new TraversalBuilder(conjunction, insertable, graphMgr);
        currentBuilder.traversal().filter(currentBuilder.retrievedResolvers());
        return currentBuilder;
    }

    private Optional<Map<Identifier.Variable.Retrievable, Set<Label>>> executeTypeResolvers(TraversalBuilder traversalBuilder) {
        return logicCache.resolver().get(traversalBuilder.traversal().structure(), structure ->
                traversalEng.combination(traversalBuilder.traversal(), thingVariableIds(traversalBuilder)).map(result -> {
                            Map<Identifier.Variable.Retrievable, Set<Label>> mapping = new HashMap<>();
                            result.forEach((id, types) -> {
                                Optional<Variable> originalVar = traversalBuilder.getOriginalVariable(id);
                                if (originalVar.isPresent()) {
                                    Set<Label> labels = mapping.computeIfAbsent(id, (i) -> new HashSet<>());
                                    types.forEach(vertex -> labels.add(vertex.properLabel()));
                                }
                            });
                            return mapping;
                        }
                )
        );
    }

    private Set<Identifier.Variable.Retrievable> thingVariableIds(TraversalBuilder traversalBuilder) {
        return iterate(traversalBuilder.resolverToOriginal.values()).filter(Variable::isThing).map(var -> {
            assert var.id().isRetrievable();
            return var.id().asRetrievable();
        }).toSet();
    }

    private static class TraversalBuilder {

        private static final Identifier.Variable ROOT_ATTRIBUTE_ID = Identifier.Variable.label(ATTRIBUTE.toString());
        private static final Identifier.Variable ROOT_THING_ID = Identifier.Variable.label(THING.toString());
        private static final Label ROOT_ATTRIBUTE_LABEL = Label.of(ATTRIBUTE.toString());
        private static final Label ROOT_THING_LABEL = Label.of(THING.toString());
        private final Map<Identifier.Variable, Set<ValueType>> resolverValueTypes;
        private final Map<Identifier.Variable, TypeVariable> originalToResolver;
        private final Map<Identifier.Variable, Variable> resolverToOriginal;
        private final GraphManager graphMgr;
        private final Conjunction conjunction;
        private final GraphTraversal.Type traversal;
        private final boolean insertable;
        private boolean hasRootAttribute;
        private boolean hasRootThing;
        private int sysVarCounter;

        TraversalBuilder(Conjunction conjunction, boolean insertable, GraphManager graphMgr) {
            this(conjunction, new GraphTraversal.Type(), 0, insertable, graphMgr);
        }

        TraversalBuilder(Conjunction conjunction, TraversalBuilder scopingTraversal, boolean insertable, GraphManager graphMgr) {
            this(conjunction, scopingTraversal.traversal(), scopingTraversal.sysVarCounter(), insertable, graphMgr);
        }

        private TraversalBuilder(Conjunction conjunction, GraphTraversal.Type initialTraversal, int initialSysVarCounter,
                                 boolean insertable, GraphManager graphMgr) {
            this.graphMgr = graphMgr;
            this.conjunction = conjunction;
            this.traversal = initialTraversal;
            this.resolverToOriginal = new HashMap<>();
            this.originalToResolver = new HashMap<>();
            this.resolverValueTypes = new HashMap<>();
            this.sysVarCounter = initialSysVarCounter;
            this.insertable = insertable;
            this.hasRootAttribute = false;
            this.hasRootThing = false;
            conjunction.variables().forEach(this::register);
        }

        public Set<Identifier.Variable.Retrievable> retrievedResolvers() {
            return iterate(resolverToOriginal.keySet()).filter(Identifier::isRetrievable).map(Identifier.Variable::asRetrievable).toSet();
        }

        public int sysVarCounter() {
            return sysVarCounter;
        }

        GraphTraversal.Type traversal() {
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
                traversal.labels(resolver.id(), var.inferredTypes());
            } else {
                resolver = var;
            }
            originalToResolver.put(var.id(), resolver);
            resolverToOriginal.putIfAbsent(resolver.id(), var);
            if (!var.inferredTypes().isEmpty()) traversal.labels(resolver.id(), var.inferredTypes());

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

        private void restrict(Identifier.Variable id, FunctionalIterator<TypeVertex> types) {
            TraversalVertex.Properties.Type props = traversal.structure().typeVertex(id).props();
            Set<Label> existingLabels = props.labels();
            if (existingLabels.isEmpty()) types.forEachRemaining(t -> existingLabels.add(t.properLabel()));
            else {
                Set<Label> intersection = types.filter(t -> existingLabels.contains(t.properLabel()))
                        .map(TypeVertex::properLabel).toSet();
                props.clearLabels();
                props.labels(intersection);
            }
        }

        private void registerOwns(TypeVariable resolver, OwnsConstraint ownsConstraint) {
            TypeVariable attrResolver = register(ownsConstraint.attribute());
            traversal.owns(resolver.id(), attrResolver.id(), ownsConstraint.isKey());
            restrict(resolver.id(), graphMgr.schema().attributeOwnerTypes());
            restrict(attrResolver.id(), graphMgr.schema().attributeTypesOwned());
        }

        private void registerPlays(TypeVariable resolver, PlaysConstraint playsConstraint) {
            TypeVariable roleResolver = register(playsConstraint.role());
            traversal.plays(resolver.id(), roleResolver.id());
            restrict(resolver.id(), graphMgr.schema().playerTypes());
            restrict(roleResolver.id(), graphMgr.schema().roleTypesPlayed());
        }

        private void registerRegex(TypeVariable resolver, RegexConstraint regexConstraint) {
            traversal.regex(resolver.id(), regexConstraint.regex().pattern());
            restrict(resolver.id(), graphMgr.schema().attributeTypes(Encoding.ValueType.STRING));
        }

        private void registerRelates(TypeVariable resolver, RelatesConstraint relatesConstraint) {
            TypeVariable roleResolver = register(relatesConstraint.role());
            traversal.relates(resolver.id(), roleResolver.id());
            restrict(resolver.id(), graphMgr.schema().relationTypes());
            restrict(resolver.id(), graphMgr.schema().roleTypes());
        }

        private void registerSub(TypeVariable resolver, SubConstraint subConstraint) {
            TypeVariable superResolver = register(subConstraint.type());
            traversal.sub(resolver.id(), superResolver.id(), !subConstraint.isExplicit());
            if (superResolver.id().isLabel()) {
                assert superResolver.label().isPresent();
                if (!subConstraint.isExplicit()) {
                    restrict(resolver.id(), graphMgr.schema().getSubtypes(
                            graphMgr.schema().getType(superResolver.label().get().properLabel())));
                } else {
                    restrict(resolver.id(), graphMgr.schema().getType(superResolver.label().get().properLabel())
                            .ins().edge(Encoding.Edge.Type.SUB).from());
                }
            }
        }

        private void registerValueType(TypeVariable resolver, ValueTypeConstraint valueTypeConstraint) {
            traversal.valueType(resolver.id(), valueTypeConstraint.valueType());
            inferTypesByValueType(resolver, valueTypeConstraint.valueType());
        }

        private void inferTypesByValueType(TypeVariable resolver, ValueType valueType) {
            switch (valueType) {
                case STRING:
                    restrict(resolver.id(), graphMgr.schema().attributeTypes(Encoding.ValueType.STRING));
                    break;
                case LONG:
                    restrict(resolver.id(), graphMgr.schema().attributeTypes(Encoding.ValueType.LONG));
                    break;
                case DOUBLE:
                    restrict(resolver.id(), graphMgr.schema().attributeTypes(Encoding.ValueType.DOUBLE));
                    break;
                case BOOLEAN:
                    restrict(resolver.id(), graphMgr.schema().attributeTypes(Encoding.ValueType.BOOLEAN));
                    break;
                case DATETIME:
                    restrict(resolver.id(), graphMgr.schema().attributeTypes(Encoding.ValueType.DATETIME));
                    break;
                default:
                    throw TypeDBException.of(ILLEGAL_STATE);
            }
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
                registerIID(resolver, constraint);
                if (resolver.constraints().isEmpty()) registerSubThing(resolver);
            });
            return resolver;
        }

        private void registerSubThing(TypeVariable resolver) {
            assert resolver.constraints().isEmpty();
            registerRootThing();
            traversal.sub(resolver.id(), ROOT_THING_ID, true);
        }

        private void registerIsa(TypeVariable resolver, IsaConstraint isaConstraint) {
            if (!isaConstraint.isExplicit() && !insertable) {
                traversal.sub(resolver.id(), register(isaConstraint.type()).id(), true);
            } else if (isaConstraint.type().id().isName() || isaConstraint.type().id().isLabel()) {
                traversal.equalTypes(resolver.id(), register(isaConstraint.type()).id());
            } else {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
            if (isaConstraint.type().label().isPresent()) {
                restrict(resolver.id(), graphMgr.schema().getSubtypes(
                        graphMgr.schema().getType(isaConstraint.type().label().get().properLabel())));
            }
        }

        private void registerIs(TypeVariable resolver, IsConstraint isConstraint) {
            traversal.equalTypes(resolver.id(), register(isConstraint.variable()).id());
        }

        /**
         * We only extract the type of the IID, and include this information in the type inference
         * We only look at the type prefix because the static type checker (this class) only read schema, not data
         */
        private void registerIID(TypeVariable resolver, IIDConstraint constraint) {
            TypeVertex type = graphMgr.schema().convert(VertexIID.Thing.of(constraint.iid()).type());
            if (type == null) throw TypeDBException.of(UNSATISFIABLE_PATTERN, conjunction, constraint);
            traversal.labels(resolver.id(), type.properLabel());
        }

        private void registerHas(TypeVariable resolver, HasConstraint hasConstraint) {
            TypeVariable attributeResolver = register(hasConstraint.attribute());
            traversal.owns(resolver.id(), attributeResolver.id(), false);
            registerSubAttribute(attributeResolver);
            restrict(resolver.id(), graphMgr.schema().attributeOwnerTypes());
            restrict(attributeResolver.id(), graphMgr.schema().attributeTypesOwned());
        }

        private void registerRelation(TypeVariable resolver, RelationConstraint constraint) {
            for (RelationConstraint.RolePlayer rolePlayer : constraint.players()) {
                TypeVariable playerResolver = register(rolePlayer.player());
                TypeVariable actingRoleResolver = new TypeVariable(newSystemId());
                if (rolePlayer.roleType().isPresent()) {
                    TypeVariable roleTypeResolver = register(rolePlayer.roleType().get());
                    traversal.sub(actingRoleResolver.id(), roleTypeResolver.id(), true);
                    restrict(roleTypeResolver.id(), graphMgr.schema().roleTypes());
                    restrict(actingRoleResolver.id(), graphMgr.schema().roleTypes());
                }
                traversal.relates(resolver.id(), actingRoleResolver.id());
                traversal.plays(playerResolver.id(), actingRoleResolver.id());
                restrict(playerResolver.id(), graphMgr.schema().playerTypes());
            }
            restrict(resolver.id(), graphMgr.schema().relationTypes());
        }

        private void registerInsertableRelation(TypeVariable resolver, RelationConstraint constraint) {
            for (RelationConstraint.RolePlayer rolePlayer : constraint.players()) {
                TypeVariable playerResolver = register(rolePlayer.player());
                TypeVariable roleResolver = register(rolePlayer.roleType().isPresent() ?
                        rolePlayer.roleType().get() : new TypeVariable(newSystemId()));
                traversal.relates(resolver.id(), roleResolver.id());
                traversal.plays(playerResolver.id(), roleResolver.id());
            }
            restrict(resolver.id(), graphMgr.schema().relationTypes());
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
                valueTypes.forEach(valueType -> {
                    traversal.valueType(resolver.id(), valueType);
                    inferTypesByValueType(resolver, valueType);
                });
                resolverValueTypes.put(resolver.id(), valueTypes);
            } else if (!resolverValueTypes.get(resolver.id()).containsAll(valueTypes)) {
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
