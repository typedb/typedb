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

package grakn.core.reasoner;

import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Label;
import grakn.core.graph.GraphManager;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.Variable;
import grakn.core.traversal.Identifier;
import graql.lang.common.GraqlArg;
import graql.lang.pattern.variable.Reference;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static graql.lang.common.GraqlToken.Type.ATTRIBUTE;
import static graql.lang.common.GraqlToken.Type.RELATION;
import static graql.lang.common.GraqlToken.Type.ROLE;
import static graql.lang.common.GraqlToken.Type.THING;

public class TypeInference {


    //TODO: for testing only. Will delete later
//    public TypeInference(Conjunction conjunction) {
//        this(conjunction, null);
//    }

    //TODO: Should not be public (fix when Unit Testing is implemented)
    //TODO: only for testing
//    public Conjunction convertConjunction() {
//        ConjunctionConverter queryCreator = new ConjunctionConverter();
//        return queryCreator.convertConjunction();
//    }

    public static void full(Conjunction conjunction, GraphManager graphManager) {
        ConstraintMapper constraintMapper = new ConstraintMapper(conjunction);
        InferenceVariables inferenceVariables = constraintMapper.getInferenceVariables();
        TypeVariable metaThing = createMetaThing(conjunction, inferenceVariables);
        Map<Reference, Set<Label>> labels = computeTypeLabels(new HashSet<>(inferenceVariables.getInferenceVariables()), graphManager);
        for (Variable variable : conjunction.variables()) {
            if (variable.isThing()) {
                addInferredIsaLabels(variable.asThing(), labels.get(variable.reference()), metaThing);
                addInferredRoleLabels(variable.asThing(), labels, inferenceVariables);
            }
        }
    }

    public static void simple(Conjunction conjunction, GraphManager graphManager) {
        ConstraintMapper constraintMapper = new ConstraintMapper(conjunction);
        InferenceVariables inferenceVariables = constraintMapper.getInferenceVariables();
        TypeVariable metaThing = createMetaThing(conjunction, inferenceVariables);
        Map<Reference, Set<Label>> labels = computeTypeLabels(new HashSet<>(inferenceVariables.getInferenceVariables()), graphManager);

        for (Variable variable : conjunction.variables()) {
            if (variable.isThing()) {
                Set<Variable> neighbourhood = new HashSet<>();
                TypeVariable typeVariable = inferenceVariables.getConversion(variable);
                neighbourhood.add(typeVariable);
                neighbourhood.addAll(constraintMapper.getVariableNeighbours().get(typeVariable));

                addInferredIsaLabels(variable.asThing(), labels.get(variable.reference()), metaThing);
                addInferredRoleLabels(variable.asThing(), computeTypeLabels(neighbourhood, graphManager), inferenceVariables);
            }
        }

    }

    private static TypeVariable createMetaThing(Conjunction conjunction, InferenceVariables inferenceVariables) {
        Label metaLabel = Label.of(THING.toString());

        Optional<TypeVariable> metaType = conjunction.variables().stream().filter(Variable::isType).map(Variable::asType)
                .filter(variable -> variable.label().isPresent())
                .filter(variable -> variable.label().get().labelLabel().equals(metaLabel))
                .findAny();

        if (metaType.isPresent()) return inferenceVariables.convert(metaType.get());
        TypeVariable thingType = new TypeVariable(Identifier.Variable.of(Reference.label(metaLabel.toString())));
        thingType.label(metaLabel);
        return thingType;
    }

    private static void addInferredIsaLabels(ThingVariable variable, Set<Label> labels, TypeVariable metaThing) {
        if (variable.isa().isEmpty()) variable.isa(metaThing, false);
        IsaConstraint isaConstraint = variable.isa().iterator().next();
        isaConstraint.labels(labels);
    }

    private static void addInferredRoleLabels(ThingVariable variable, Map<Reference, Set<Label>> labels, InferenceVariables inferenceVariables) {
        Set<RelationConstraint> relationConstraints = variable.asThing().relation();
        for (RelationConstraint constraint : relationConstraints) {
            List<RelationConstraint.RolePlayer> rolePlayers = constraint.players();
            for (RelationConstraint.RolePlayer rolePlayer : rolePlayers) {
                Variable typeVariable;
                if (rolePlayer.roleType().isPresent() && rolePlayer.roleType().get().reference().isName()) {
                    typeVariable = rolePlayer.roleType().get();
                } else {
                    typeVariable = inferenceVariables.getConversion(rolePlayer);
                }
                rolePlayer.labels(labels.get(typeVariable.reference()));
            }
        }
    }


    private static Map<Reference, Set<Label>> computeTypeLabels(Set<Variable> inferenceVariables, GraphManager graphManager) {
        Conjunction inference = new Conjunction(inferenceVariables, Collections.emptySet());

        Map<Reference, Set<Label>> mapping = new HashMap<>();
        inference.traversal().execute(graphManager).forEachRemaining(result ->
                result.forEach((ref, vertex) -> {
                    mapping.putIfAbsent(ref, new HashSet<>());
                    mapping.get(ref).add(Label.of(vertex.asType().label(), vertex.asType().scope()));
                })
        );

        //used for testing mocking very simple inference of types.
//        for (Variable variable : conjunction.variables()) {
//            TypeVariable asTypeVar = variableMapping.get(variable);
//            if (asTypeVar.sub().isEmpty() || asTypeVar.sub().iterator().next().type().reference().isName()) {
//                mapping.put(variable.reference(), Collections.singleton(Label.of(THING.toString())));
//            } else {
//                TypeVariable subType = asTypeVar.sub().iterator().next().type();
//                mapping.put(variable.reference(), Collections.singleton(subType.label().get().labelLabel()));
//            }
//
//        }
        return mapping;
    }

    private static class ConstraintMapper {
        private InferenceVariables inferenceVariables;
        private HashMap<TypeVariable, Set<TypeVariable>> neighbours;
        private TypeVariable metaThing;
        private TypeVariable metaAttribute;
        private TypeVariable metaRelation;
        private TypeVariable metaRole;
        private Conjunction conjunction;

        public ConstraintMapper(Conjunction conjunction) {
            this.inferenceVariables = new InferenceVariables();
            this.neighbours = new HashMap<>();
            this.conjunction = conjunction;
            this.metaThing = createMeta(Label.of(THING.toString()));
            this.metaAttribute = createMeta(Label.of(ATTRIBUTE.toString()));
            this.metaRelation = createMeta(Label.of(RELATION.toString()));
            this.metaRole = createMeta(Label.of(ROLE.toString(), RELATION.toString()));
            conjunction.variables().forEach(this::convertVariable);
            inferenceVariables.getInferenceVariables().forEach(this::putSubThingConstraintIfAbsent);
        }

        private TypeVariable createMeta(Label metaLabel) {

            Optional<TypeVariable> metaType = conjunction.variables().stream().filter(Variable::isType).map(Variable::asType)
                    .filter(variable -> variable.label().isPresent())
                    .filter(variable -> variable.label().get().labelLabel().equals(metaLabel))
                    .findAny();

            if (metaType.isPresent()) return inferenceVariables.convert(metaType.get());
            TypeVariable newMetaType = new TypeVariable(Identifier.Variable.of(Reference.label(metaLabel.toString())));
            newMetaType.label(metaLabel);
            return newMetaType;
        }

        public InferenceVariables getInferenceVariables() {
            return inferenceVariables;
        }

        public HashMap<TypeVariable, Set<TypeVariable>> getVariableNeighbours() {
            return neighbours;
        }

        private void putSubThingConstraintIfAbsent(TypeVariable variable) {
            if (isMapped(variable)) variable.sub(metaThing, true);
        }

        private boolean isMapped(TypeVariable variable) {
            return !variable.reference().isLabel() && variable.sub().isEmpty() &&
                    !variable.label().isPresent() && variable.is().isEmpty();
        }

        private TypeVariable convertVariable(Variable variable) {
            if (inferenceVariables.hasConversion(variable)) return inferenceVariables.getConversion(variable);
            if (variable.isType()) {
                TypeVariable asTypeVar = inferenceVariables.convert(variable);
                neighbours.putIfAbsent(asTypeVar, new HashSet<>());
            } else convertThingVariable(variable.asThing());
            return inferenceVariables.getConversion(variable);
        }

        private void convertThingVariable(ThingVariable thingVariable) {
            TypeVariable inferenceVariable = inferenceVariables.convert(thingVariable);
            neighbours.putIfAbsent(inferenceVariable, new HashSet<>());

            if (thingVariable.constraints().isEmpty()) return;

            thingVariable.isa().forEach(constraint -> convertIsa(inferenceVariable, constraint));
            thingVariable.is().forEach(constraint -> convertIs(inferenceVariable, constraint));
            thingVariable.has().forEach(constraint -> convertHas(inferenceVariable, constraint));
            thingVariable.value().forEach(constraint -> convertValue(inferenceVariable, constraint));
            thingVariable.relation().forEach(constraint -> convertRelation(inferenceVariable, constraint));
        }

        private void convertRelation(TypeVariable owner, RelationConstraint relationConstraint) {
            if (isMapped(owner)) owner.sub(metaRelation, true);
            ThingVariable ownerThing = relationConstraint.owner();
            TypeVariable relationTypeVar;
            if (ownerThing.isa().isEmpty()) {
                relationTypeVar = inferenceVariables.newInferenceVariable();
                neighbours.put(relationTypeVar, new HashSet<>());
            } else {
                relationTypeVar = convertVariable(ownerThing.isa().iterator().next().type());
            }
            if (isMapped(relationTypeVar)) relationTypeVar.sub(metaRelation, true);
            for (RelationConstraint.RolePlayer rolePlayer : relationConstraint.players()) {
                TypeVariable playerType = convertVariable(rolePlayer.player());
                TypeVariable roleTypeVar = rolePlayer.roleType().orElse(null);
                if (roleTypeVar != null) {
                    roleTypeVar = convertVariable(roleTypeVar);
                    if (isMapped(roleTypeVar)) roleTypeVar.sub(metaRole, true);
                    addRelatesConstraint(owner, roleTypeVar);
                    addRelatesConstraint(relationTypeVar, roleTypeVar);
                }

                if (roleTypeVar == null || roleTypeVar.reference().isLabel()) {
                    TypeVariable rpInferenceVariable = inferenceVariables.convert(rolePlayer);
                    neighbours.put(rpInferenceVariable, new HashSet<>());
                    if (isMapped(rpInferenceVariable)) rpInferenceVariable.sub(metaRole, true);
                    addRelatesConstraint(owner, rpInferenceVariable);
                    addRelatesConstraint(relationTypeVar, rpInferenceVariable);
                    playerType.plays(null, rpInferenceVariable, null);
                    addNeighbour(playerType, rpInferenceVariable);
                } else {
                    playerType.plays(null, roleTypeVar, null);
                    addNeighbour(playerType, roleTypeVar);
                }
            }
        }

        private void addRelatesConstraint(TypeVariable owner, TypeVariable roleType) {
            if (owner != null && !owner.reference().isLabel()) owner.relates(roleType, null);
            if (owner != null) addNeighbour(owner, roleType);
        }

        private void convertHas(TypeVariable owner, HasConstraint hasConstraint) {
            TypeVariable attributeTypeVar = convertVariable(hasConstraint.attribute());
            owner.owns(attributeTypeVar, null, false);
            if (isMapped(attributeTypeVar)) attributeTypeVar.sub(metaAttribute, true);
            addNeighbour(owner, attributeTypeVar);
        }

        private void convertIsa(TypeVariable owner, IsaConstraint isaConstraint) {
            TypeVariable isaVar = convertVariable(isaConstraint.type());
            if (!isaConstraint.isExplicit()) owner.sub(isaConstraint.type(), false);
            else if (isaConstraint.type().reference().isName()) owner.is(isaConstraint.type());
            else if (isaConstraint.type().label().isPresent())
                owner.label(isaConstraint.type().label().get().labelLabel());
            else GraknException.of(ILLEGAL_STATE);
            addNeighbour(isaVar, isaVar);
        }

        private void convertIs(TypeVariable owner, grakn.core.pattern.constraint.thing.IsConstraint isConstraint) {
            TypeVariable isVar = convertVariable(isConstraint.variable());
            owner.is(isVar);
            addNeighbour(owner, isVar);
        }

        private void convertValue(TypeVariable owner, ValueConstraint<?> constraint) {
            if (constraint.isBoolean()) owner.valueType(GraqlArg.ValueType.BOOLEAN);
            else if (constraint.isString()) owner.valueType(GraqlArg.ValueType.STRING);
            else if (constraint.isDateTime()) owner.valueType(GraqlArg.ValueType.DATETIME);
            else if (constraint.isDouble()) owner.valueType(GraqlArg.ValueType.DOUBLE);
            else if (constraint.isLong()) owner.valueType(GraqlArg.ValueType.LONG);
            else throw GraknException.of(ILLEGAL_STATE);
            if (isMapped(owner)) owner.sub(metaAttribute, true);
        }

        public void addNeighbour(TypeVariable from, TypeVariable to) {
            neighbours.get(from).add(to);
            neighbours.get(to).add(from);
        }

    }

    private static class InferenceVariables {

        private Map<Reference, TypeVariable> variableInferenceVars;
        private Map<RelationConstraint.RolePlayer, TypeVariable> rpInferenceVars;

        private Integer tempCounter;

        InferenceVariables() {
            this.tempCounter = 0;
            this.variableInferenceVars = new HashMap<>();
        }

        TypeVariable newInferenceVariable() {
            TypeVariable tempVar = new TypeVariable(Identifier.Variable.of(
                    new grakn.core.pattern.variable.Reference.System("temp" + addAndGetCounter())));
            variableInferenceVars.put(tempVar.reference(), tempVar);
            return tempVar;
        }

        public TypeVariable convert(RelationConstraint.RolePlayer key) {
            if (!rpInferenceVars.containsKey(key)) {
                TypeVariable newTypeVar = new TypeVariable(Identifier.Variable.of(
                        new grakn.core.pattern.variable.Reference.System("temp" + addAndGetCounter())));
                rpInferenceVars.put(key, newTypeVar);
            }
            return rpInferenceVars.get(key);
        }

        public TypeVariable convert(Variable key) {
            if (!variableInferenceVars.containsKey(key.reference())) {
                TypeVariable newTypeVar = new TypeVariable(key.identifier());
                if (key.isType()) key.asType().constraints().forEach(newTypeVar::constrain);
                variableInferenceVars.put(key.reference(), newTypeVar);
            }
            return variableInferenceVars.get(key.reference());
        }


        public TypeVariable getConversion(Variable key) {
            return variableInferenceVars.get(key.reference());
        }

        public boolean hasConversion(Variable key) {
            return variableInferenceVars.containsKey(key.reference());
        }

        public Collection<TypeVariable> getInferenceVariables() {
            return variableInferenceVars.values();
        }

        private Integer addAndGetCounter() {
            return ++tempCounter;
        }

        public Variable getConversion(RelationConstraint.RolePlayer rolePlayer) {
            return rpInferenceVars.get(rolePlayer);
        }
    }


}
