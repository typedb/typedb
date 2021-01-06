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
import java.util.Map;
import java.util.Set;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.TypeRead.ROLE_TYPE_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static grakn.core.common.iterator.Iterators.iterate;

//TODO: here we remake Type Resolver, using a Traversal Structure instead of a Pattern to move on the graph and find out answers.
public class TypeResolverTraversal {

    private final ConceptManager conceptMgr;
    private final TraversalEngine traversalEng;
    private final LogicCache logicCache;

    public TypeResolverTraversal(ConceptManager conceptMgr, TraversalEngine traversalEng, LogicCache logicCache) {
        this.conceptMgr = conceptMgr;
        this.traversalEng = traversalEng;
        this.logicCache = logicCache;
    }

    public Conjunction resolveVariables(Conjunction conjunction) {
        resolveLabels(conjunction);
        ConstraintMapper constraintMapper = new ConstraintMapper(conjunction, conceptMgr);
        if (constraintMapper.isSatisfiable) runTraversalEngine(constraintMapper);
        conjunction.variables().stream().filter(variable -> variable.resolvedTypes().isEmpty())
                .forEach(variable -> variable.setSatisfiable(false));
        long numOfTypes = traversalEng.graph().schema().stats().thingTypeCount();
        long numOfConcreteTypes = traversalEng.graph().schema().stats().concreteThingTypeCount();
        conjunction.variables().stream().filter(variable -> (
                variable.isType() && variable.resolvedTypes().size() == numOfTypes ||
                        variable.isThing() && variable.resolvedTypes().size() == numOfConcreteTypes
        )).forEach(Variable::clearResolvedTypes);
        return conjunction;
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

    private void runTraversalEngine(ConstraintMapper constraintMapper) {
        traversalEng.iterator(constraintMapper.traversal).forEachRemaining(
                //TODO: take this logic into its own method.
                result -> result.forEach((ref, vertex) -> {
                    Variable variable = constraintMapper.referenceVariableMap.get(ref);
                    if ((variable.isType() || !vertex.asType().isAbstract()))
                        variable.addResolvedType(Label.of(vertex.asType().label(), vertex.asType().scope()));
                })
        );
    }

    //TODO: renaming to reflect Traversal Structure
    private static class ConstraintMapper {

        private final Map<Reference, Variable> referenceVariableMap;
        private final Map<Identifier, TypeVariable> resolvers;
        private final Traversal traversal;
        private int sysVarCounter;
        private final ConceptManager conceptMgr;
        private final Map<Identifier, Set<ValueType>> valueTypeRegister;
        private boolean isSatisfiable;

        ConstraintMapper(Conjunction conjunction, ConceptManager conceptMgr) {
            this.conceptMgr = conceptMgr;
            this.traversal = new Traversal();
            this.referenceVariableMap = new HashMap<>();
            this.resolvers = new HashMap<>();
            this.sysVarCounter = 0;
            this.isSatisfiable = true;
            this.valueTypeRegister = new HashMap<>();
            conjunction.variables().forEach(this::convert);
//            conjunction.variables().forEach(variable -> referenceVariableMap.putIfAbsent(variable.reference(), variable));
        }

        private void convert(Variable variable) {
            if (variable.isType()) convert(variable.asType());
            else if (variable.isThing()) convert(variable.asThing());
            else throw GraknException.of(ILLEGAL_STATE);
        }

        private TypeVariable convert(TypeVariable variable) {
            if (resolvers.containsKey(variable.id())) return resolvers.get(variable.id());
            resolvers.put(variable.id(), variable);
            referenceVariableMap.putIfAbsent(variable.reference(), variable);
            variable.addTo(traversal);
            return variable;
        }

        private TypeVariable convert(ThingVariable variable) {
            if (resolvers.containsKey(variable.id())) return resolvers.get(variable.id());

            TypeVariable resolver = new TypeVariable(variable.reference().isAnonymous() ?
                                                             newSystemId() : variable.id());
            resolvers.put(variable.id(), resolver);
            referenceVariableMap.putIfAbsent(resolver.reference(), variable);
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
            traversal.labels(owner.id(), conceptMgr.getThing(iidConstraint.iid()).getType().getLabel());
        }

        private void convertIsa(TypeVariable owner, IsaConstraint isaConstraint) {
            if (!isaConstraint.isExplicit()) traversal.sub(owner.id(), convert(isaConstraint.type()).id(), true);
            else if (isaConstraint.type().reference().isName())
                traversal.equalTypes(owner.id(), convert(isaConstraint.type()).id());
            else if (isaConstraint.type().label().isPresent())
                traversal.labels(owner.id(), isaConstraint.type().label().get().properLabel());
        }

        private void convertIs(TypeVariable owner, IsConstraint isConstraint) {
            traversal.equalTypes(owner.id(), convert(isConstraint.variable()).id());
        }

        private void convertHas(TypeVariable owner, HasConstraint hasConstraint) {
            traversal.owns(owner.id(), convert(hasConstraint.attribute()).id(), false);
        }

        private void convertValue(TypeVariable owner, ValueConstraint<?> constraint) {
            Set<ValueType> valueTypes = comparableValue(constraint);
            if (valueTypeRegister.get(owner.id()).isEmpty()) {
                valueTypes.forEach(valueType -> traversal.valueType(owner.id(), valueType));
                valueTypeRegister.put(owner.id(), valueTypes);
            } else if (!valueTypeRegister.get(owner.id()).containsAll(valueTypes)) this.isSatisfiable = false;
            subAttribute(owner.id());
        }

        private Set<ValueType> comparableValue(ValueConstraint<?> constraint) {
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
            traversal.labels(attributeID, Label.of("attribute"));
            traversal.sub(variable, attributeID, true);
        }

        private void convertRelation(TypeVariable owner, RelationConstraint constraint) {
            for (RelationConstraint.RolePlayer rolePlayer : constraint.players()) {
                TypeVariable playerType = convert(rolePlayer.player());
                TypeVariable playingRoleType = convert(new TypeVariable(newSystemId()));
                if (rolePlayer.roleType().isPresent()) {
                    TypeVariable roleTypeVar = convert(rolePlayer.roleType().get());
                    traversal.sub(playingRoleType.id(), roleTypeVar.id(), true);
                }
                traversal.relates(owner.id(), playingRoleType.id());
                traversal.plays(playerType.id(), playingRoleType.id());
            }
        }

        private Identifier.Variable newSystemId() {
            return Identifier.Variable.of(SystemReference.of(sysVarCounter++));
        }

    }


}
