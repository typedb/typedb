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
import grakn.core.concept.type.Type;
import grakn.core.logic.LogicCache;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IIDConstraint;
import grakn.core.pattern.constraint.thing.IsConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.constraint.type.AbstractConstraint;
import grakn.core.pattern.constraint.type.LabelConstraint;
import grakn.core.pattern.constraint.type.OwnsConstraint;
import grakn.core.pattern.constraint.type.PlaysConstraint;
import grakn.core.pattern.constraint.type.RegexConstraint;
import grakn.core.pattern.constraint.type.RelatesConstraint;
import grakn.core.pattern.constraint.type.SubConstraint;
import grakn.core.pattern.constraint.type.ValueTypeConstraint;
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
import java.util.Optional;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.TypeRead.OVERRIDDEN_TYPES_IN_TRAVERSAL;

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
        ConstraintMapper constraintMapper = new ConstraintMapper(conjunction, conceptMgr);

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

    //TODO: renaming to reflect Traversal Structure
    private static class ConstraintMapper {

        private Map<Reference, Variable> referenceVariableMap;
        private Map<Identifier, TypeVariable> resolvers;
        private Traversal traversal;
        private int sysVarCounter;
        private ConceptManager conceptMgr;

        ConstraintMapper(Conjunction conjunction, ConceptManager conceptMgr) {
            this.conceptMgr = conceptMgr;
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
            if (resolvers.containsKey(variable.id())) return resolvers.get(variable.id());
            TypeVariable resolver;
            if (variable.reference().isAnonymous()) resolver = new TypeVariable(newSystemId());
            else resolver = new TypeVariable(variable.id());
            variable.label().ifPresent(constraint -> convertLabel(resolver, constraint));
            variable.valueType().ifPresent(constraint ->  convertValueType(resolver, constraint));
            variable.regex().ifPresent(constraint -> convertRegex(resolver, constraint));
            if (variable.abstractConstraint().isPresent()) traversal.isAbstract(resolver.id());
            variable.sub().ifPresent(constraint -> convertSub(resolver, constraint));
            variable.owns().forEach(constraint -> convertOwns(resolver, constraint));
            variable.plays().forEach(constraint -> convertPlays(resolver, constraint));
            variable.relates().forEach(constraint -> convertRelates(resolver, constraint));
            variable.is().forEach(constraint -> convertIsType(resolver, constraint));
            return resolver;
        }

        private void convertLabel(TypeVariable owner, LabelConstraint constraint) {
            traversal.labels(owner.id(), constraint.properLabel());
        }

        private void convertValueType(TypeVariable owner, ValueTypeConstraint constraint) {
            traversal.valueType(owner.id(), constraint.valueType());
        }

        private void convertRegex(TypeVariable owner, RegexConstraint constraint) {
            traversal.regex(owner.id(), constraint.regex().pattern());
        }

        private void convertSub(TypeVariable owner, SubConstraint constraint) {
            traversal.sub(owner.id(), constraint.type().id(), !constraint.isExplicit());
        }

        private void convertOwns(TypeVariable owner, OwnsConstraint constraint) {
            if (constraint.overridden().isPresent()) throw GraknException.of(OVERRIDDEN_TYPES_IN_TRAVERSAL);
            traversal.owns(owner.id(), constraint.attribute().id(), constraint.isKey());
        }

        private void convertPlays(TypeVariable owner, PlaysConstraint constraint) {
            if (constraint.overridden().isPresent()) throw GraknException.of(OVERRIDDEN_TYPES_IN_TRAVERSAL);
            traversal.plays(owner.id(), constraint.role().id());
        }

        private void convertRelates(TypeVariable owner, RelatesConstraint constraint) {
            if (constraint.overridden().isPresent()) throw GraknException.of(OVERRIDDEN_TYPES_IN_TRAVERSAL);
            traversal.relates(owner.id(), constraint.role().id());
        }

        private void convertIsType(TypeVariable owner, grakn.core.pattern.constraint.type.IsConstraint constraint) {
            traversal.equalTypes(owner.id(), constraint.variable().id());
        }

        private TypeVariable convert(ThingVariable variable) {
            if (resolvers.containsKey(variable.id())) return resolvers.get(variable.id());

            TypeVariable resolver = new TypeVariable(variable.id());
            resolvers.put(variable.id(), resolver);

            variable.isa().ifPresent(constraint -> convertIsa(resolver, constraint));
            variable.is().forEach(constraint -> convertIs(resolver, constraint));
            variable.has().forEach(constraint -> convertHas(resolver, constraint));
            variable.value().forEach(constraint -> convertValue(resolver, constraint));
            variable.relation().forEach(constraint -> convertRelation(resolver, constraint));
            variable.iid().ifPresent(constraint -> convertIID(resolver, constraint));
            return resolver;
        }

        private void convertIID(TypeVariable owner, IIDConstraint iidConstraint) {
            //TODO: implement as 'retrival' in case the label has already appeared.
            assert conceptMgr.getThing(iidConstraint.iid()) != null;
            traversal.labels(owner.id(), conceptMgr.getThing(iidConstraint.iid()).getType().getLabel());
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
            //TODO: renaming of ownerThing. Also, do we even need it?
            ThingVariable ownerThing = constraint.owner();
            convert(ownerThing);

            for (RelationConstraint.RolePlayer rolePlayer : constraint.players()) {
                TypeVariable playerType = convert(rolePlayer.player());
                TypeVariable roleTypeVar = rolePlayer.roleType()
                        .flatMap(typeVariable -> Optional.of(convert(typeVariable))).orElse(null);
                if (roleTypeVar != null) {
                    TypeVariable roleTypeVarSub = new TypeVariable(newSystemId());
                    traversal.sub(roleTypeVarSub.id(), roleTypeVar.id(), true);
                    traversal.relates(owner.id(), roleTypeVarSub.id());
                    traversal.plays(playerType.id(), roleTypeVarSub.id());
                } else {
                    roleTypeVar = new TypeVariable(newSystemId());
                    traversal.relates(owner.id(), roleTypeVar.id());
                    traversal.plays(playerType.id(), roleTypeVar.id());
                }
            }

        }

    }


}
