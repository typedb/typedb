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

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.FunctionalIterator;
import grakn.core.common.parameters.Label;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.thing.Thing;
import grakn.core.graph.common.Encoding;
import grakn.core.graph.vertex.TypeVertex;
import grakn.core.graph.vertex.Vertex;
import grakn.core.logic.LogicCache;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.pattern.Negation;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IIDConstraint;
import grakn.core.pattern.constraint.thing.IsConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.constraint.type.LabelConstraint;
import grakn.core.pattern.constraint.type.OwnsConstraint;
import grakn.core.pattern.constraint.type.PlaysConstraint;
import grakn.core.pattern.constraint.type.RegexConstraint;
import grakn.core.pattern.constraint.type.RelatesConstraint;
import grakn.core.pattern.constraint.type.SubConstraint;
import grakn.core.pattern.constraint.type.TypeConstraint;
import grakn.core.pattern.constraint.type.ValueTypeConstraint;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.UNSATISFIABLE_PATTERN;
import static grakn.core.common.exception.ErrorMessage.TypeRead.ROLE_TYPE_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static grakn.core.common.iterator.Iterators.iterate;
import static graql.lang.common.GraqlToken.Type.ATTRIBUTE;
import static graql.lang.common.GraqlToken.Type.THING;

public class TypeResolver {

    private final ConceptManager conceptMgr;
    private final TraversalEngine traversalEng;
    private final LogicCache logicCache;

    public TypeResolver(ConceptManager conceptMgr, TraversalEngine traversalEng, LogicCache logicCache) {
        this.conceptMgr = conceptMgr;
        this.traversalEng = traversalEng;
        this.logicCache = logicCache;
    }

    public FunctionalIterator<Map<Identifier.Variable.Name, Label>> namedCombinations(Conjunction conjunction, boolean insertable) {
        Traversal resolverTraversal = new Traversal();
        TraversalBuilder traversalBuilder = new TraversalBuilder(conjunction, conceptMgr, resolverTraversal, 0, insertable);
        resolverTraversal.filter(traversalBuilder.retrievedResolvers());
        return traversalEng.iterator(traversalBuilder.traversal()).map(vertexMap -> {
            Map<Identifier.Variable.Name, Label> mapping = new HashMap<>();
            vertexMap.forEach((id, vertex) -> {
                assert vertex.isType();
                traversalBuilder.getVariable(id).map(Variable::id).filter(Identifier::isName)
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
                        if (labels.isEmpty()) throw GraknException.of(ROLE_TYPE_NOT_FOUND, label.name(), scope);
                        typeVar.addResolvedTypes(labels);
                    } else {
                        TypeVertex type = traversalEng.graph().schema().getType(label);
                        if (type == null) throw GraknException.of(TYPE_NOT_FOUND, label);
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
        Traversal resolverTraversal = new Traversal();
        TraversalBuilder traversalBuilder = builder(resolverTraversal, conjunction, scopingConjunctions, insertable);
        resolverTraversal.filter(traversalBuilder.retrievedResolvers());
        Map<Identifier.Variable.Retrievable, Set<Label>> resolvedLabels = executeResolverTraversals(traversalBuilder);
        if (resolvedLabels.isEmpty()) conjunction.setCoherent(false);
        else {
            resolvedLabels.forEach((id, labels) -> traversalBuilder.getVariable(id).ifPresent(variable -> {
                assert variable.resolvedTypes().isEmpty() || variable.resolvedTypes().containsAll(labels);
                variable.setResolvedTypes(labels);
            }));
        }
    }

    private boolean isSchemaQuery(Conjunction conjunction) {
        return iterate(conjunction.variables()).noneMatch(Variable::isThing);
    }

    private TraversalBuilder builder(Traversal traversal, Conjunction conjunction, List<Conjunction> scopingConjunctions,
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

    private Map<Identifier.Variable.Retrievable, Set<Label>> executeResolverTraversals(TraversalBuilder traversalBuilder) {
        return logicCache.resolver().get(traversalBuilder.traversal(), traversal -> {
            Map<Identifier.Variable.Retrievable, Set<Label>> mapping = new HashMap<>();
            traversalEng.iterator(traversal, true).forEachRemaining(
                    result -> {
                        // TODO: This filter should not be needed if we enforce traversal only to return non-abstract
                        assert iterate(result.map().values()).allMatch(Vertex::isType);
                        if (iterate(result.map().values()).noneMatch(typeVertex -> typeVertex.asType().isAbstract())) {
                            result.forEach((id, vertex) -> {
                                mapping.putIfAbsent(id, new HashSet<>());
                                if (!traversalBuilder.getVariable(id).isPresent()) return;
                                if (traversalBuilder.getVariable(id).get().isThing()) {
                                    mapping.get(id).add(vertex.asType().properLabel());
                                }
                            });
                        }
                    }
            );
            return mapping;
        });
    }

    private static class TraversalBuilder {

        private static final Identifier.Variable ROOT_ATTRIBUTE_ID = Identifier.Variable.of(Reference.label(ATTRIBUTE.toString()));
        private static final Label ROOT_ATTRIBUTE_LABEL = Label.of(ATTRIBUTE.toString());
        private static final Label ROOT_THING_LABEL = Label.of(THING.toString());
        private final Map<Identifier.Variable, Variable> resolvers;
        private final Map<Identifier.Variable, Set<ValueType>> resolverValueTypes;
        private final Map<Identifier.Variable, TypeVariable> originalToResolver;
        private final ConceptManager conceptMgr;
        private final Conjunction conjunction;
        private final Traversal traversal;
        private final boolean insertable;
        private boolean hasRootAttribute;
        private int sysVarCounter;

        TraversalBuilder(Conjunction conjunction, ConceptManager conceptMgr, Traversal initialTraversal,
                         int initialAnonymousVarCounter, boolean insertable) {
            this.conceptMgr = conceptMgr;
            this.conjunction = conjunction;
            this.traversal = initialTraversal;
            this.resolvers = new HashMap<>();
            this.originalToResolver = new HashMap<>();
            this.resolverValueTypes = new HashMap<>();
            this.sysVarCounter = initialAnonymousVarCounter;
            this.insertable = insertable;
            this.hasRootAttribute = false;
            conjunction.variables().forEach(this::register);
        }

        public Set<Identifier.Variable.Retrievable> retrievedResolvers() {
            return iterate(resolvers.keySet()).filter(Identifier::isRetrievable).map(Identifier.Variable::asRetrievable).toSet();
        }

        public int sysVarCounter() {
            return sysVarCounter;
        }

        Traversal traversal() {
            return traversal;
        }

        Optional<Variable> getVariable(Identifier.Variable id) {
            return Optional.ofNullable(resolvers.get(id));
        }

        private void register(Variable variable) {
            if (variable.isType()) register(variable.asType());
            else if (variable.isThing()) register(variable.asThing());
            else throw GraknException.of(ILLEGAL_STATE);
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
            resolvers.putIfAbsent(resolver.id(), var);
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
                else if (!constraint.isLabel()) throw GraknException.of(ILLEGAL_STATE);
            }

            return resolver;
        }

        private void registerAbstract(TypeVariable resolver) {
            traversal.isAbstract(resolver.id());
        }

        private void registerIsType(TypeVariable resolver, grakn.core.pattern.constraint.type.IsConstraint isConstraint) {
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
            resolvers.putIfAbsent(resolver.id(), var);
            resolverValueTypes.putIfAbsent(resolver.id(), set());

            // Note: order is important! convertValue assumes that any other Variable encountered from that edge will
            // have resolved its valueType, so we execute convertValue first.
            var.value().forEach(constraint -> registerValue(resolver, constraint));
            var.isa().ifPresent(constraint -> registerIsa(resolver, constraint));
            var.is().forEach(constraint -> registerIsThing(resolver, constraint));
            var.has().forEach(constraint -> registerHas(resolver, constraint));
            if (insertable) var.relation().forEach(constraint -> registerInsertableRelation(resolver, constraint));
            else var.relation().forEach(constraint -> registerRelation(resolver, constraint));
            var.iid().ifPresent(constraint -> registerIID(resolver, constraint));
            return resolver;
        }

        private void registerIID(TypeVariable resolver, IIDConstraint iidConstraint) {
            Thing thing = conceptMgr.getThing(iidConstraint.iid());
            if (thing != null) traversal.labels(resolver.id(), thing.getType().getLabel());
        }

        private void registerIsa(TypeVariable resolver, IsaConstraint isaConstraint) {
            if (!isaConstraint.isExplicit() && !insertable)
                traversal.sub(resolver.id(), register(isaConstraint.type()).id(), true);
            else if (isaConstraint.type().reference().isName())
                traversal.equalTypes(resolver.id(), register(isaConstraint.type()).id());
            else if (isaConstraint.type().label().isPresent())
                traversal.labels(resolver.id(), isaConstraint.type().label().get().properLabel());
            else throw GraknException.of(ILLEGAL_STATE);
        }

        private void registerIsThing(TypeVariable resolver, IsConstraint isConstraint) {
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
                        .map(Encoding.ValueType::graqlValueType).toSet();
            }

            if (resolverValueTypes.get(resolver.id()).isEmpty()) {
                valueTypes.forEach(valueType -> traversal.valueType(resolver.id(), valueType));
                resolverValueTypes.put(resolver.id(), valueTypes);
            } else if (!resolverValueTypes.get(resolver.id()).containsAll(valueTypes)) {
                // TODO this is a bit odd - can we set not coherent here and short circuit?
                throw GraknException.of(UNSATISFIABLE_PATTERN, conjunction, constraint);
            }
            registerSubAttribute(resolver);
        }

        private void registerSubAttribute(Variable resolver) {
            assert resolvers.get(resolver.id()).isThing();
            Optional<IsaConstraint> isa = resolvers.get(resolver.id()).asThing().isa();
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


        private Identifier.Variable newSystemId() {
            return Identifier.Variable.of(SystemReference.of(sysVarCounter++));
        }
    }
}
