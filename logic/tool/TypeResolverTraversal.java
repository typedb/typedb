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
import grakn.core.concept.thing.Thing;
import grakn.core.logic.LogicCache;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IsConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.constraint.type.LabelConstraint;
import grakn.core.pattern.variable.SystemReference;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.Variable;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
import graql.lang.common.GraqlArg;
import graql.lang.pattern.variable.Reference;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

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
        //TODO: main API
        ConstraintMapper constraintMapper = new ConstraintMapper(conjunction);

        runTraversalEngine(constraintMapper);
        return conjunction;
    }

    private void runTraversalEngine(ConstraintMapper constraintMapper) {
        traversalEng.iterator(constraintMapper.traversal).forEachRemaining(
                result -> result.forEach((ref, vertex) -> {
                    Variable variable = constraintMapper.referenceVariableMap.get(ref);
                    variable.addResolvedType(Label.of(vertex.asType().label(), vertex.asType().scope()));
                })
        );
    }


    private static class ConstraintMapper {

        private Map<Reference, Variable> referenceVariableMap;
        private Map<Identifier, TypeVariable> resolvers;
        private Traversal traversal;
        private int sysVarCounter;

        ConstraintMapper(Conjunction conjunction) {
            this.traversal = new Traversal();
            this.referenceVariableMap = new HashMap<>();
            this.resolvers = new HashMap<>();
            this.sysVarCounter = 0;
            conjunction.variables().forEach(this::convert);
            conjunction.variables().forEach(variable -> referenceVariableMap.putIfAbsent(variable.reference(), variable));
        }

        private void convert(Variable variable) {
            if (variable.isType()) convert(variable.asType());
            else if(variable.isThing()) convert(variable.asThing());
            else throw GraknException.of(ILLEGAL_STATE);
        }

        private Identifier.Variable newSystemId() {
            return Identifier.Variable.of(SystemReference.of(sysVarCounter++));
        }

        private TypeVariable convert(TypeVariable variable) {
            //TODO
            if (resolvers.containsKey(variable.id())) return resolvers.get(variable.id());
            TypeVariable resolver;
            if (variable.reference().isAnonymous()) resolver = new TypeVariable(newSystemId());
            else resolver = new TypeVariable(variable.id());
//            if (resolvers.containsKey(variable.id())) return resolvers.get(variable.id());
//            Structure resolver = resolvers.
            variable.label().ifPresent(constraint -> convertLabel(variable, constraint));
            return resolver;
        }

        private void convertLabel(TypeVariable owner, LabelConstraint constraint) {
            traversal.labels(owner.id(), constraint.properLabel());
        }

        private TypeVariable convert(ThingVariable variable) {
            //TODO
            if (resolvers.containsKey(variable.id())) return resolvers.get(variable.id());

            TypeVariable resolver = new TypeVariable(variable.id());
            resolvers.put(variable.id(), resolver);

            variable.isa().ifPresent(constraint -> convertIsa(resolver, constraint));
            variable.is().forEach(constraint -> convertIs(resolver, constraint));
            variable.has().forEach(constraint -> convertHas(resolver, constraint));
            variable.value().forEach(constraint -> convertValue(resolver, constraint));
            variable.relation().forEach(constraint -> convertRelation(resolver, constraint));
            return resolver;
        }

        private void convertIsa(TypeVariable owner, IsaConstraint isaConstraint) {
            if (!isaConstraint.isExplicit())traversal.sub(owner.id(), convert(isaConstraint.type()).id(), true);
            else if (isaConstraint.type().reference().isName()) traversal.equalTypes(owner.id(), convert(isaConstraint.type()).id());
            else if (isaConstraint.type().label().isPresent()) traversal.labels(owner.id(), isaConstraint.type().label().get().properLabel());
        }

        private void convertIs(TypeVariable owner, IsConstraint isConstraint) {
            traversal.equalTypes(owner.id(), convert(isConstraint.variable()).id());
        }

        private void convertHas(TypeVariable owner, HasConstraint hasConstraint) {
            traversal.owns(owner.id(), convert(hasConstraint.attribute()).id(), false);
        }

        private void convertValue(TypeVariable owner, ValueConstraint<?> constraint) {
            if (constraint.isBoolean()) traversal.valueType(owner.id(), GraqlArg.ValueType.BOOLEAN);
            else if (constraint.isString()) traversal.valueType(owner.id(), GraqlArg.ValueType.STRING);
            else if (constraint.isDateTime()) traversal.valueType(owner.id(), GraqlArg.ValueType.DATETIME);
            else if (constraint.isDouble() || constraint.isLong()){
                traversal.valueType(owner.id(), GraqlArg.ValueType.DOUBLE);
                traversal.valueType(owner.id(), GraqlArg.ValueType.LONG);
            }
            else if (constraint.isVariable()) convert(constraint.asVariable().value()); //TODO: how to capture ValueType of other Var
            else throw GraknException.of(ILLEGAL_STATE);
        }

        private void convertRelation(TypeVariable owner, RelationConstraint constraint) {
            //TODO: renaming of this
            ThingVariable ownerThing = constraint.owner();


        }

    }


}
