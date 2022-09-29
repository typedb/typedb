/*
 * Copyright (C) 2022 Vaticle
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

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.iid.VertexIID;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.logic.LogicCache;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.pattern.constraint.thing.HasConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.IIDConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.IsConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.IsaConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.RelationConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.ValueConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.OwnsConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.PlaysConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.RegexConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.RelatesConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.SubConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.TypeConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.ValueTypeConstraint;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import com.vaticle.typedb.core.traversal.common.Modifiers;
import com.vaticle.typedb.core.traversal.graph.TraversalVertex;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.UNSATISFIABLE_PATTERN_VARIABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.UNSATISFIABLE_PATTERN_VARIABLE_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.UNSATISFIABLE_SUB_PATTERN;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.ROLE_TYPE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typeql.lang.common.TypeQLToken.Type.ATTRIBUTE;

public class TypeInference {

    private final LogicCache logicCache;
    private final GraphManager graphMgr;
    private final TraversalEngine traversalEng;

    public TypeInference(LogicCache logicCache, TraversalEngine traversalEng, GraphManager graphMgr) {
        this.logicCache = logicCache;
        this.graphMgr = graphMgr;
        this.traversalEng = traversalEng;
    }

    public void applyCombination(Disjunction disjunction) {
        applyCombination(disjunction, new HashMap<>());
    }

    private void applyCombination(Disjunction disjunction, Map<Identifier.Variable.Name, Set<Label>> bounds) {
        disjunction.conjunctions().forEach(conjunction -> applyCombination(conjunction, bounds, false));
    }

    public void applyCombination(Conjunction conjunction) {
        applyCombination(conjunction, false);
    }

    public void applyCombination(Conjunction conjunction, boolean insertable) {
        applyCombination(conjunction, new HashMap<>(), insertable);
    }

    private void applyCombination(Conjunction conjunction, Map<Identifier.Variable.Name, Set<Label>> bounds, boolean insertable) {
        propagateLabels(conjunction);
        if (!bounds.isEmpty()) applyBounds(conjunction, bounds);
        Map<Retrievable.Name, Set<Label>> inferredTypes = new HashMap<>(bounds);
        if (!isSchemaQuery(conjunction)) {
            new InferenceTraversal(conjunction, insertable, graphMgr, traversalEng).applyCombination(logicCache);
            inferredTypes.putAll(namedInferredTypes(conjunction));
        }
        conjunction.negations().forEach(negation -> applyCombination(negation.disjunction(), inferredTypes));
    }

    public FunctionalIterator<Map<Identifier.Variable.Name, Label>> getPermutations(Conjunction conjunction,
                                                                                    boolean insertable,
                                                                                    Set<Identifier.Variable.Name> filter) {
        propagateLabels(conjunction);
        return new InferenceTraversal(conjunction, insertable, graphMgr, traversalEng).typePermutations(filter);
    }

    private void applyBounds(Conjunction conjunction, Map<Identifier.Variable.Name, Set<Label>> bounds) {
        conjunction.variables().forEach(var -> {
            if (var.id().isName() && bounds.containsKey(var.id().asName())) {
                if (var.inferredTypes().isEmpty()) var.addInferredTypes(bounds.get(var.id().asName()));
                else var.retainInferredTypes(bounds.get(var.id().asName()));
            }
        });
    }

    private boolean isSchemaQuery(Conjunction conjunction) {
        return iterate(conjunction.variables()).noneMatch(Variable::isThing);
    }

    private Map<Retrievable.Name, Set<Label>> namedInferredTypes(Conjunction conjunction) {
        Map<Retrievable.Name, Set<Label>> namedInferences = new HashMap<>();
        iterate(conjunction.variables()).filter(var -> var.id().isName()).forEachRemaining(var ->
                namedInferences.put(var.id().asName(), var.inferredTypes())
        );
        return namedInferences;
    }

    private void propagateLabels(Conjunction conj) {
        iterate(conj.variables()).filter(v -> v.isType() && v.asType().label().isPresent()).forEachRemaining(typeVar -> {
            Label label = typeVar.asType().label().get().properLabel();
            if (label.scope().isPresent()) {
                Set<Label> labels = graphMgr.schema().resolveRoleTypeLabels(label);
                if (labels.isEmpty()) throw TypeDBException.of(ROLE_TYPE_NOT_FOUND, label.name(), label.scope().get());
                typeVar.addInferredTypes(labels);
            } else {
                if (graphMgr.schema().getType(label) == null) throw TypeDBException.of(TYPE_NOT_FOUND, label);
                typeVar.addInferredTypes(label);
            }
        });
    }

    private static class InferenceTraversal {

        private static final Identifier.Variable ROOT_ATTRIBUTE_ID = Identifier.Variable.label(ATTRIBUTE.toString());

        private final Conjunction conjunction;
        private final GraphManager graphMgr;
        private final TraversalEngine traversalEng;
        private final boolean insertable;

        private final GraphTraversal.Type traversal;
        private final Map<Identifier.Variable, TypeVariable> originalToInference;
        private final Map<Retrievable, Variable> inferenceToOriginal;
        private final Map<Pair<Identifier.Variable, RelationConstraint.RolePlayer>, TypeVariable> rolePlayerToInference;
        private int nextGeneratedID;

        private InferenceTraversal(Conjunction conjunction, boolean insertable, GraphManager graphMgr,
                                   TraversalEngine traversalEng) {
            this.conjunction = conjunction;
            this.graphMgr = graphMgr;
            this.traversalEng = traversalEng;
            this.insertable = insertable;
            this.traversal = new GraphTraversal.Type();
            this.originalToInference = new HashMap<>();
            this.inferenceToOriginal = new HashMap<>();
            this.rolePlayerToInference = new HashMap<>();
            this.nextGeneratedID = largestAnonymousVar(conjunction) + 1;
            conjunction.variables().forEach(this::register);
            traversal.filter(Modifiers.Filter.create(iterate(inferenceToOriginal.keySet())
                    .link(iterate(rolePlayerToInference.values()).map(var -> var.id().asRetrievable())).toSet()
            ));
        }

        private FunctionalIterator<Map<Identifier.Variable.Name, Label>> typePermutations(Set<Identifier.Variable.Name> filter) {
            Modifiers.Filter inferenceFilter = Modifiers.Filter.create(iterate(filter).map(id -> originalToInference.get(id).id().asRetrievable()).toSet());
            traversal.modifiers().filter(inferenceFilter).sorting(Modifiers.Sorting.EMPTY);
            return traversalEng.iterator(traversal).map(vertexMap -> {
                Map<Retrievable.Name, Label> labels = new HashMap<>();
                vertexMap.forEach((id, vertex) -> {
                    if (!inferenceToOriginal.containsKey(id)) return;
                    Identifier.Variable originalID = inferenceToOriginal.get(id).id();
                    if (originalID.isName()) labels.put(originalID.asName(), vertex.asType().properLabel());
                });
                return labels;
            });
        }

        private void applyCombination(LogicCache logicCache) {
            Optional<Map<Retrievable, Set<Label>>> inferredTypes = logicCache.inference().get(
                    traversal,
                    traversal -> traversalEng.combination(traversal, thingInferenceVars()).map(types -> {
                        HashMap<Retrievable, Set<Label>> labels = new HashMap<>();
                        types.forEach((id, ts) -> labels.put(id, iterate(ts).map(TypeVertex::properLabel).toSet()));
                        return labels;
                    })
            );
            if (inferredTypes.isPresent()) applyTypes(inferredTypes.get());
            else conjunction.setCoherent(false);
        }

        private Set<Identifier.Variable.Retrievable> thingInferenceVars() {
            return iterate(conjunction.variables()).filter(Variable::isThing).map(var ->
                    originalToInference.get(var.id()).id().asRetrievable()
            ).toSet();
        }

        private void applyTypes(Map<Retrievable, Set<Label>> types) {
            inferenceToOriginal.forEach((inferenceID, conjunctionVar) ->
                    conjunctionVar.setInferredTypes(types.get(inferenceID))
            );
            iterate(conjunction.variables()).filter(var -> var.isThing() && var.asThing().relation().isPresent())
                    .flatMap(var -> iterate(var.asThing().relation().get().players()).map(rp -> new Pair<>(var.id(), rp)))
                    .forEachRemaining(ownerAndRP -> {
                        assert rolePlayerToInference.containsKey(ownerAndRP);
                        ownerAndRP.second().setInferredRoleTypes(types.get(rolePlayerToInference.get(ownerAndRP).id().asRetrievable()));
                    });
        }

        private static int largestAnonymousVar(Conjunction conjunction) {
            return iterate(conjunction.variables()).filter(var -> var.id().isAnonymous())
                    .map(var -> var.id().asAnonymous().anonymousId()).stream().max(Comparator.naturalOrder()).orElse(0);
        }

        private void register(Variable variable) {
            if (variable.isType()) register(variable.asType());
            else if (variable.isThing()) register(variable.asThing());
            else throw TypeDBException.of(ILLEGAL_STATE);
        }

        private TypeVariable register(TypeVariable var) {
            if (originalToInference.containsKey(var.id())) return originalToInference.get(var.id());

            Retrievable inferenceID = newID();
            TypeVariable inferenceVar = new TypeVariable(inferenceID);
            originalToInference.put(var.id(), inferenceVar);
            inferenceToOriginal.putIfAbsent(inferenceID, var);
            if (!var.inferredTypes().isEmpty()) restrictTypes(inferenceVar.id(), iterate(var.inferredTypes()));

            for (TypeConstraint constraint : var.constraints()) {
                if (constraint.isAbstract()) registerAbstract(inferenceVar);
                else if (constraint.isIs()) registerIsType(inferenceVar, constraint.asIs());
                else if (constraint.isOwns()) registerOwns(inferenceVar, constraint.asOwns());
                else if (constraint.isPlays()) registerPlays(inferenceVar, constraint.asPlays());
                else if (constraint.isRegex()) registerRegex(inferenceVar, constraint.asRegex());
                else if (constraint.isRelates()) registerRelates(inferenceVar, constraint.asRelates());
                else if (constraint.isSub()) registerSub(inferenceVar, constraint.asSub());
                else if (constraint.isValueType()) registerValueType(inferenceVar, constraint.asValueType());
                else if (!constraint.isLabel()) throw TypeDBException.of(ILLEGAL_STATE);
            }
            return inferenceVar;
        }

        private void registerAbstract(TypeVariable resolver) {
            traversal.isAbstract(resolver.id());
        }

        private void registerIsType(TypeVariable resolver, com.vaticle.typedb.core.pattern.constraint.type.IsConstraint isConstraint) {
            traversal.equalTypes(resolver.id(), register(isConstraint.variable()).id());
        }

        private void registerOwns(TypeVariable inferenceVar, OwnsConstraint ownsConstraint) {
            TypeVariable attrVar = register(ownsConstraint.attribute());
            traversal.owns(inferenceVar.id(), attrVar.id(), ownsConstraint.isKey());
        }

        private void registerPlays(TypeVariable inferenceVar, PlaysConstraint playsConstraint) {
            TypeVariable roleVar = register(playsConstraint.role());
            traversal.plays(inferenceVar.id(), roleVar.id());
        }

        private void registerRegex(TypeVariable inferenceVar, RegexConstraint regexConstraint) {
            traversal.regex(inferenceVar.id(), regexConstraint.regex().pattern());
            restrictTypes(inferenceVar.id(), graphMgr.schema().attributeTypes(Encoding.ValueType.STRING)
                    .map(TypeVertex::properLabel));
        }

        private void registerRelates(TypeVariable inferenceVar, RelatesConstraint relatesConstraint) {
            TypeVariable roleVar = register(relatesConstraint.role());
            traversal.relates(inferenceVar.id(), roleVar.id());
        }

        private void registerSub(TypeVariable inferenceVar, SubConstraint subConstraint) {
            TypeVariable superVar = register(subConstraint.type());
            traversal.sub(inferenceVar.id(), superVar.id(), !subConstraint.isExplicit());
        }

        private void registerValueType(TypeVariable resolver, ValueTypeConstraint valueTypeConstraint) {
            traversal.valueType(resolver.id(), valueTypeConstraint.valueType());
        }

        private TypeVariable register(ThingVariable var) {
            if (originalToInference.containsKey(var.id())) return originalToInference.get(var.id());

            Retrievable inferenceID = var.id();
            TypeVariable inferenceVar = new TypeVariable(inferenceID);
            originalToInference.put(var.id(), inferenceVar);
            inferenceToOriginal.putIfAbsent(inferenceID, var);
            if (!var.inferredTypes().isEmpty()) restrictTypes(inferenceVar.id(), iterate(var.inferredTypes()));

            var.value().forEach(constraint -> registerValue(inferenceVar, constraint));
            var.isa().ifPresent(constraint -> registerIsa(inferenceVar, constraint));
            var.is().forEach(constraint -> registerIs(inferenceVar, constraint));
            var.has().forEach(constraint -> registerHas(inferenceVar, constraint));
            if (insertable) {
                var.relation().ifPresent(constraint -> registerInsertableRelation(inferenceVar, constraint));
            } else var.relation().ifPresent(constraint -> registerRelation(inferenceVar, constraint));
            var.iid().ifPresent(constraint -> registerIID(inferenceVar, constraint));
            return inferenceVar;
        }

        private void registerValue(TypeVariable inferenceVar, ValueConstraint<?> constraint) {
            Set<Encoding.ValueType<?>> predicateValueTypes;
            if (constraint.isVariable()) {
                TypeVariable var = register(constraint.asVariable().value());
                registerSubAttribute(var);
                predicateValueTypes = traversal.structure().typeVertex(var.id()).props().valueTypes();
            } else {
                predicateValueTypes = set(Encoding.ValueType.of(constraint.value().getClass()));
            }

            Set<Encoding.ValueType<?>> valueTypes = iterate(predicateValueTypes)
                    .flatMap(valueType -> iterate(valueType.comparables())).toSet();
            if (!valueTypes.isEmpty()) restrictValueTypes(inferenceVar.id(), iterate(valueTypes));
            else registerSubAttribute(inferenceVar);
        }

        private void registerIsa(TypeVariable inferenceVar, IsaConstraint isaConstraint) {
            if (!isaConstraint.isExplicit() && !insertable) {
                traversal.sub(inferenceVar.id(), register(isaConstraint.type()).id(), true);
            } else if (isaConstraint.type().id().isName() || isaConstraint.type().id().isLabel()) {
                traversal.equalTypes(inferenceVar.id(), register(isaConstraint.type()).id());
            } else {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
        }

        private void registerIs(TypeVariable inferenceVar, IsConstraint isConstraint) {
            traversal.equalTypes(inferenceVar.id(), register(isConstraint.variable()).id());
        }

        /**
         * We only extract the type of the IID, and include this information in the type inference
         * We only look at the type prefix because the static type checker (this class) only read schema, not data
         */
        private void registerIID(TypeVariable resolver, IIDConstraint constraint) {
            TypeVertex type = graphMgr.schema().convert(VertexIID.Thing.of(constraint.iid()).type());
            if (type == null) {
                conjunction.setCoherent(false);
                throw TypeDBException.of(UNSATISFIABLE_SUB_PATTERN, conjunction, constraint);
            }
            restrictTypes(resolver.id(), iterate(type).map(TypeVertex::properLabel));
        }

        private void registerHas(TypeVariable inferenceVar, HasConstraint hasConstraint) {
            TypeVariable attrVar = register(hasConstraint.attribute());
            traversal.owns(inferenceVar.id(), attrVar.id(), false);
        }

        private void registerRelation(TypeVariable inferenceVar, RelationConstraint constraint) {
            for (RelationConstraint.RolePlayer rolePlayer : constraint.players()) {
                TypeVariable playerVar = register(rolePlayer.player());
                TypeVariable roleVar = registerRolePlayer(constraint.owner().id(), rolePlayer);
                if (rolePlayer.roleType().isPresent()) {
                    TypeVariable roleTypeVar;
                    roleTypeVar = register(rolePlayer.roleType().get());
                    traversal.sub(roleVar.id(), roleTypeVar.id(), true);
                }
                traversal.relates(inferenceVar.id(), roleVar.id());
                traversal.plays(playerVar.id(), roleVar.id());
            }
        }

        private void registerInsertableRelation(TypeVariable inferenceVar, RelationConstraint constraint) {
            for (RelationConstraint.RolePlayer rolePlayer : constraint.players()) {
                TypeVariable playerVar = register(rolePlayer.player());
                TypeVariable roleTypeVar;
                TypeVariable roleVar = registerRolePlayer(constraint.owner().id(), rolePlayer);
                if (rolePlayer.roleType().isPresent()) {
                    roleTypeVar = register(rolePlayer.roleType().get());
                } else {
                    roleTypeVar = new TypeVariable(newID());
                }
                traversal.equalTypes(roleVar.id(), roleTypeVar.id());
                traversal.relates(inferenceVar.id(), roleTypeVar.id());
                traversal.plays(playerVar.id(), roleTypeVar.id());
            }
        }

        private TypeVariable registerRolePlayer(Retrievable ownerID, RelationConstraint.RolePlayer rolePlayer) {
            Pair<Identifier.Variable, RelationConstraint.RolePlayer> ownerAndPlayer = new Pair<>(ownerID, rolePlayer);
            if (rolePlayerToInference.containsKey(ownerAndPlayer)) return rolePlayerToInference.get(ownerAndPlayer);
            TypeVariable inferenceVar = new TypeVariable(newID());
            rolePlayerToInference.put(ownerAndPlayer, inferenceVar);
            return inferenceVar;
        }

        private void registerSubAttribute(Variable inferenceVar) {
            traversal.labels(ROOT_ATTRIBUTE_ID, Label.of(ATTRIBUTE.toString()));
            traversal.sub(inferenceVar.id(), ROOT_ATTRIBUTE_ID, true);
        }

        private void restrictTypes(Identifier.Variable id, FunctionalIterator<Label> labels) {
            TraversalVertex.Properties.Type props = traversal.structure().typeVertex(id).props();
            if (props.labels().isEmpty()) labels.forEachRemaining(props.labels()::add);
            else {
                Set<Label> intersection = labels.filter(props.labels()::contains).toSet();
                props.clearLabels();
                props.labels(intersection);
            }
            if (props.labels().isEmpty()) {
                conjunction.setCoherent(false);
                throw TypeDBException.of(UNSATISFIABLE_PATTERN_VARIABLE, conjunction, inferenceToOriginal.get(id.asRetrievable()));
            }
        }

        private void restrictValueTypes(Identifier.Variable id, FunctionalIterator<Encoding.ValueType<?>> valueTypes) {
            TraversalVertex.Properties.Type props = traversal.structure().typeVertex(id).props();
            if (props.valueTypes().isEmpty()) valueTypes.forEachRemaining(props.valueTypes()::add);
            else {
                Set<Encoding.ValueType<?>> intersection = valueTypes.filter(props.valueTypes()::contains).toSet();
                props.clearValueTypes();
                props.valueTypes(intersection);
            }
            if (props.valueTypes().isEmpty()) {
                conjunction.setCoherent(false);
                throw TypeDBException.of(UNSATISFIABLE_PATTERN_VARIABLE_VALUE, conjunction, inferenceToOriginal.get(id.asRetrievable()));
            }
        }

        private Retrievable newID() {
            return Identifier.Variable.anon(nextGeneratedID++);
        }
    }
}
